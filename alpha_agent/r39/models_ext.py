"""alpha_agent.r39.models_ext - Track G: the $0 model-frontier completion.

Every materially distinct model family that is genuinely executable at $0
on this workstation, executed; every family that is not, blocked for a
NAMED, measured reason (``continuation.FOUNDATION_MODEL_BLOCKERS``).
Redundancy is not diversity: no third boosting implementation enters.

New families (all admitted here, none in the v1 zoo):

* ``mlp``             - a small feed-forward neural regressor
                        (NEURAL_TABULAR; sklearn, BSD-3).
* ``calibrated_sign`` - an isotonic-CALIBRATED probability model over the
                        excess sign (CALIBRATED_PROBABILITY; sklearn).
* ``quantile_blend``  - a distributional location estimate, the mean of
                        LightGBM q10/q50/q90 heads (DISTRIBUTIONAL beyond
                        the v1 single-quantile tail model).
* ``tcn_seq``         - a small causal temporal convolutional net over each
                        market's trailing 12-decision feature sequence
                        (DEEP_SEQUENCE; torch CPU, trained from RANDOM
                        INITIALISATION - no pretrained weights).
* ``gru_seq``         - a small recurrent benchmark over the same
                        sequences (DEEP_SEQUENCE / recurrent).
* ``ssl_embed_ridge`` - masked-autoencoder embeddings (torch, from
                        scratch) feeding a ridge head
                        (SELF_SUPERVISED_LITE).

torch is imported from the research-drive CPU-only install
(``continuation.TORCH_LIB_DIR``); if it is unavailable the deep adapters
raise ``TorchUnavailable`` and the campaign records the branch blocked
with that reason rather than pretending it ran.
"""
from __future__ import annotations

import sys

import numpy as np

from .continuation import TORCH_LIB_DIR
from .model_registry import Adapter

CALCULATION_OWNER = "alpha_agent.r39.models_ext"

SEQ_N_LAGS = 11          # current decision + 11 trailing = 12 steps
TORCH_EPOCHS = 40
TORCH_BATCH = 1024
TORCH_LR = 1e-3


class TorchUnavailable(RuntimeError):
    pass


def _torch():
    if str(TORCH_LIB_DIR) not in sys.path:
        sys.path.insert(0, str(TORCH_LIB_DIR))
    try:
        import torch
    except Exception as e:  # pragma: no cover - environment-dependent
        raise TorchUnavailable(str(e))
    torch.set_num_threads(4)
    return torch


# --------------------------------------------------------------------------- #
# Sequence features (lagged panel columns; the matrix stays the interface)
# --------------------------------------------------------------------------- #
def add_sequence_lags(panel, cols: list, *, n_lags: int = SEQ_N_LAGS,
                      id_col: str = "market_id") -> tuple:
    """Adds ``<col>_lag<k>`` columns per market (k = 1..n_lags) and returns
    (panel, ordered sequence column list). Order is feature-major with time
    ascending: [f_lag<n>, ..., f_lag1, f] per feature."""
    panel = panel.sort_values([id_col, "decision_date"])
    seq_cols = []
    g = panel.groupby(id_col)
    for c in cols:
        for k in range(n_lags, 0, -1):
            name = "%s_lag%d" % (c, k)
            if name not in panel.columns:
                panel[name] = g[c].shift(k)
            seq_cols.append(name)
        seq_cols.append(c)
    return panel.sort_values(["decision_date", id_col]) \
        .reset_index(drop=True), seq_cols


class _Standardiser:
    def fit(self, X):
        X = np.asarray(X, dtype=np.float64)
        self.med = np.nanmedian(X, axis=0)
        self.med = np.where(np.isfinite(self.med), self.med, 0.0)
        Xi = np.where(np.isfinite(X), X, self.med)
        self.mu = Xi.mean(axis=0)
        sd = Xi.std(axis=0, ddof=1)
        self.sd = np.where(sd > 0, sd, 1.0)
        return (Xi - self.mu) / self.sd

    def apply(self, X):
        X = np.asarray(X, dtype=np.float64)
        Xi = np.where(np.isfinite(X), X, self.med)
        return (Xi - self.mu) / self.sd


class SeqNetAdapter:
    """TCN or GRU over (n_steps, n_feats) sequences, trained from scratch."""

    family = "DEEP_SEQUENCE"

    def __init__(self, kind: str, n_feats: int, *, seed: int = 3903):
        self.kind = kind
        self.n_feats = n_feats
        self.n_steps = SEQ_N_LAGS + 1
        self.seed = seed
        self.prep = _Standardiser()
        self.name = kind

    def _build(self, torch):
        nn = torch.nn
        f, s = self.n_feats, self.n_steps
        if self.kind == "tcn_seq":
            return nn.Sequential(
                nn.Conv1d(f, 16, kernel_size=3, padding=2, dilation=1),
                nn.ReLU(),
                nn.Conv1d(16, 16, kernel_size=3, padding=4, dilation=2),
                nn.ReLU(),
                nn.AdaptiveAvgPool1d(1), nn.Flatten(),
                nn.Linear(16, 1))
        if self.kind == "gru_seq":
            class GRUHead(nn.Module):
                def __init__(self):
                    super().__init__()
                    self.gru = nn.GRU(f, 16, batch_first=True)
                    self.out = nn.Linear(16, 1)

                def forward(self, x):  # x: (N, steps, feats)
                    _, h = self.gru(x)
                    return self.out(h[-1])
            return GRUHead()
        raise ValueError(self.kind)

    def _tensor(self, torch, X):
        Z = self.prep.apply(X) if hasattr(self.prep, "mu") else None
        Z = Z if Z is not None else X
        N = Z.shape[0]
        seq = Z.reshape(N, self.n_feats, self.n_steps)  # feature-major cols
        if self.kind == "gru_seq":
            seq = np.transpose(seq, (0, 2, 1))          # (N, steps, feats)
        return torch.tensor(seq, dtype=torch.float32)

    def fit(self, X, y):
        torch = _torch()
        torch.manual_seed(self.seed)
        y = np.asarray(y, dtype=np.float64)
        Z = self.prep.fit(X)
        ys = (y - np.nanmean(y)) / (np.nanstd(y) + 1e-12)
        N = Z.shape[0]
        seq = Z.reshape(N, self.n_feats, self.n_steps)
        if self.kind == "gru_seq":
            seq = np.transpose(seq, (0, 2, 1))
        xt = torch.tensor(seq, dtype=torch.float32)
        yt = torch.tensor(ys, dtype=torch.float32).view(-1, 1)
        self.net = self._build(torch)
        opt = torch.optim.Adam(self.net.parameters(), lr=TORCH_LR)
        loss_fn = torch.nn.MSELoss()
        idx = np.arange(N)
        rng = np.random.default_rng(self.seed)
        self.net.train()
        for _ in range(TORCH_EPOCHS):
            rng.shuffle(idx)
            for k in range(0, N, TORCH_BATCH):
                b = idx[k: k + TORCH_BATCH]
                opt.zero_grad()
                out = self.net(xt[b])
                loss = loss_fn(out, yt[b])
                loss.backward()
                opt.step()
        return self

    def predict(self, X):
        torch = _torch()
        self.net.eval()
        with torch.no_grad():
            out = self.net(self._tensor(torch, X)).numpy().ravel()
        return np.asarray(out, dtype=np.float64)


class SSLEmbedRidge:
    """Masked autoencoder (from scratch, torch CPU) -> ridge head."""

    family = "SELF_SUPERVISED_LITE"
    name = "ssl_embed_ridge"

    def __init__(self, *, dim: int = 16, seed: int = 3903):
        self.dim = dim
        self.seed = seed
        self.prep = _Standardiser()

    def fit(self, X, y):
        torch = _torch()
        from sklearn.linear_model import Ridge
        torch.manual_seed(self.seed)
        Z = self.prep.fit(X)
        F = Z.shape[1]
        nn = torch.nn
        self.enc = nn.Linear(F, self.dim)
        dec = nn.Linear(self.dim, F)
        params = list(self.enc.parameters()) + list(dec.parameters())
        opt = torch.optim.Adam(params, lr=TORCH_LR)
        loss_fn = nn.MSELoss()
        xt = torch.tensor(Z, dtype=torch.float32)
        N = Z.shape[0]
        rng = np.random.default_rng(self.seed)
        idx = np.arange(N)
        for _ in range(60):
            rng.shuffle(idx)
            for k in range(0, N, TORCH_BATCH):
                b = idx[k: k + TORCH_BATCH]
                xb = xt[b]
                mask = torch.tensor(
                    (rng.random(xb.shape) < 0.3).astype("float32"))
                opt.zero_grad()
                recon = dec(torch.relu(self.enc(xb * (1 - mask))))
                loss = loss_fn(recon * mask, xb * mask)
                loss.backward()
                opt.step()
        with torch.no_grad():
            H = torch.relu(self.enc(xt)).numpy()
        self.head = Ridge(alpha=10.0).fit(H, np.asarray(y, dtype=float))
        return self

    def predict(self, X):
        torch = _torch()
        Z = self.prep.apply(X)
        with torch.no_grad():
            H = torch.relu(self.enc(
                torch.tensor(Z, dtype=torch.float32))).numpy()
        return np.asarray(self.head.predict(H), dtype=np.float64)


class QuantileBlend:
    """Mean of LightGBM q10/q50/q90 heads - a distributional location."""

    family = "QUANTILE_DISTRIBUTIONAL"
    name = "quantile_blend"

    def __init__(self, *, seed: int = 3903):
        self.seed = seed

    def fit(self, X, y):
        import lightgbm as lgb
        X = np.asarray(X, dtype=np.float64)
        y = np.asarray(y, dtype=np.float64)
        self.heads = []
        for q in (0.10, 0.50, 0.90):
            m = lgb.LGBMRegressor(
                objective="quantile", alpha=q, n_estimators=300,
                learning_rate=0.03, num_leaves=15, min_child_samples=60,
                subsample=0.8, colsample_bytree=0.8,
                random_state=self.seed, n_jobs=4, verbose=-1)
            m.fit(X, y)
            self.heads.append(m)
        return self

    def predict(self, X):
        X = np.asarray(X, dtype=np.float64)
        return np.mean(np.column_stack(
            [m.predict(X) for m in self.heads]), axis=1)


class _QuietMLP:
    """MLPRegressor that treats non-convergence at the iteration cap as a
    fact, not an error - the cap IS the declared training budget."""

    def __init__(self, **kw):
        from sklearn.neural_network import MLPRegressor
        self._m = MLPRegressor(**kw)

    def fit(self, X, y):
        import warnings

        from sklearn.exceptions import ConvergenceWarning
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", ConvergenceWarning)
            self._m.fit(X, y)
        return self

    def predict(self, X):
        return self._m.predict(X)


def make_ext_adapter(name: str, *, seed: int = 3903, n_feats: int = None):
    from sklearn.calibration import CalibratedClassifierCV
    from sklearn.ensemble import HistGradientBoostingClassifier
    if name == "mlp":
        return Adapter("mlp", "NEURAL_TABULAR", True,
                       lambda: _QuietMLP(
                           hidden_layer_sizes=(32, 16), alpha=1e-3,
                           learning_rate_init=1e-3, max_iter=300,
                           random_state=seed))
    if name == "calibrated_sign":
        return Adapter(
            "calibrated_sign", "CALIBRATED_PROBABILITY", True,
            lambda: CalibratedClassifierCV(
                HistGradientBoostingClassifier(
                    max_iter=150, max_leaf_nodes=15, min_samples_leaf=60,
                    random_state=seed),
                method="isotonic", cv=3),
            classifier=True)
    if name == "quantile_blend":
        return QuantileBlend(seed=seed)
    if name in ("tcn_seq", "gru_seq"):
        if n_feats is None:
            raise ValueError("sequence adapters need n_feats")
        return SeqNetAdapter(name, n_feats, seed=seed)
    if name == "ssl_embed_ridge":
        return SSLEmbedRidge(seed=seed)
    raise KeyError(name)


EXT_MODEL_FAMILIES = {
    "mlp": "NEURAL_TABULAR",
    "calibrated_sign": "CALIBRATED_PROBABILITY",
    "quantile_blend": "QUANTILE_DISTRIBUTIONAL",
    "tcn_seq": "DEEP_SEQUENCE (causal TCN, from scratch)",
    "gru_seq": "DEEP_SEQUENCE (recurrent benchmark, from scratch)",
    "ssl_embed_ridge": "SELF_SUPERVISED_LITE (masked AE, from scratch)",
}
