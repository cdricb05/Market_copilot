"""
alpha_agent/collectors — Stage 2 source collectors.

COLLECTOR_CLASSES maps source_id -> collector class in the pre-registered
priority order defined by the Stage 2 configuration.
"""
from .base import BaseCollector, CollectorContext, RawArchive, default_transport
from .eodhd import EodhdCollector
from .finra import FinraCollector
from .fred_alfred import FredAlfredCollector
from .gdelt import GdeltCollector
from .nasdaq_trader import NasdaqTraderCollector
from .norgate_local import NorgateLocalCollector
from .sec_edgar import SecEdgarCollector

COLLECTOR_CLASSES = {
    NorgateLocalCollector.source_id: NorgateLocalCollector,
    EodhdCollector.source_id: EodhdCollector,
    SecEdgarCollector.source_id: SecEdgarCollector,
    FinraCollector.source_id: FinraCollector,
    NasdaqTraderCollector.source_id: NasdaqTraderCollector,
    FredAlfredCollector.source_id: FredAlfredCollector,
    GdeltCollector.source_id: GdeltCollector,
}

__all__ = [
    "BaseCollector", "CollectorContext", "RawArchive", "default_transport",
    "COLLECTOR_CLASSES", "NorgateLocalCollector", "EodhdCollector",
    "SecEdgarCollector", "FinraCollector", "NasdaqTraderCollector",
    "FredAlfredCollector", "GdeltCollector",
]
