"""alpha_agent.r31._versions - recorded software versions for the campaign contract.

A result is only reproducible against the library that produced it. The campaign
therefore records the versions it actually ran on, and records ABSENT libraries
too: "no gradient-boosting library was installed" is a material fact about which
known methods could be reproduced, and it belongs in the contract rather than in
a footnote.
"""
from __future__ import annotations

import importlib

#: Libraries whose presence or absence changes which known methods are
#: reproducible. Declared as a fixed tuple so the contract hash is stable.
RECORDED = ("numpy", "pandas", "scipy", "sklearn", "statsmodels",
            "lightgbm", "xgboost", "torch", "cvxpy")


def software_versions() -> dict:
    out: dict = {}
    for name in RECORDED:
        try:
            mod = importlib.import_module(name)
        except Exception:
            out[name] = "ABSENT"
            continue
        out[name] = str(getattr(mod, "__version__", "PRESENT_VERSION_UNKNOWN"))
    return out


def absent() -> list:
    v = software_versions()
    return sorted(k for k, s in v.items() if s == "ABSENT")
