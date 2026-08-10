"""
alpha_agent/collectors — Stage 2 source collectors.

COLLECTOR_CLASSES maps source_id -> collector class in the pre-registered
priority order defined by the Stage 2 configuration.
"""
from .base import BaseCollector, CollectorContext, RawArchive, default_transport
from .bea import BeaCollector
from .bls import BlsCollector
from .eodhd import EodhdCollector
from .eodhd_analyst import EodhdAnalystCollector
from .finra import FinraCollector
from .fred_alfred import FredAlfredCollector
from .gdelt import GdeltCollector
from .intrinio import IntrinioCollector
from .nasdaq_trader import NasdaqTraderCollector
from .norgate_local import NorgateLocalCollector
from .rss_atom import RssAtomCollector
from .sec_edgar import SecEdgarCollector
from .us_treasury import UsTreasuryCollector

COLLECTOR_CLASSES = {
    NorgateLocalCollector.source_id: NorgateLocalCollector,
    EodhdCollector.source_id: EodhdCollector,
    EodhdAnalystCollector.source_id: EodhdAnalystCollector,
    SecEdgarCollector.source_id: SecEdgarCollector,
    FinraCollector.source_id: FinraCollector,
    NasdaqTraderCollector.source_id: NasdaqTraderCollector,
    FredAlfredCollector.source_id: FredAlfredCollector,
    UsTreasuryCollector.source_id: UsTreasuryCollector,
    BlsCollector.source_id: BlsCollector,
    BeaCollector.source_id: BeaCollector,
    GdeltCollector.source_id: GdeltCollector,
    # Intrinio TRIAL (research-only). Registered so the machinery can DISCOVER it
    # for the explicit operator probe/acquire entrypoint; deliberately absent from
    # configs/alpha_agent/stage2_ingestion.json so the scheduled Collect cadence
    # NEVER runs it.
    IntrinioCollector.source_id: IntrinioCollector,
}

__all__ = [
    "BaseCollector", "CollectorContext", "RawArchive", "default_transport",
    "COLLECTOR_CLASSES", "NorgateLocalCollector", "EodhdCollector",
    "EodhdAnalystCollector",
    "SecEdgarCollector", "FinraCollector", "NasdaqTraderCollector",
    "FredAlfredCollector", "UsTreasuryCollector", "BlsCollector",
    "BeaCollector", "GdeltCollector", "RssAtomCollector", "IntrinioCollector",
]
