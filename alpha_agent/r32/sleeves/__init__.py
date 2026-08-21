"""alpha_agent.r32.sleeves - the six Release-32 strategy sleeves.

Every module here obeys :mod:`alpha_agent.r32.sleeve`: it turns owned,
point-in-time-admissible observations into a sequence of
:class:`~alpha_agent.r32.sleeve.StrategyOpportunity`. None of them sizes a book,
writes an allocation, creates a proposal or decision, promotes a model, or
activates anything. The architecture audit asserts that against every file in
this package.

===========================  =================================================
sleeve                       module
===========================  =================================================
EQUITY_SELECTION (control)   :mod:`alpha_agent.r32.sleeves.equity_selection`
EQUITY_BETA_TIMING           :mod:`alpha_agent.r32.sleeves.equity_beta_timing`
SECTOR_ROTATION              :mod:`alpha_agent.r32.sleeves.sector_rotation`
CROSS_ASSET_TREND            :mod:`alpha_agent.r32.sleeves.cross_asset_trend`
VOLATILITY_RISK_REGIME       :mod:`alpha_agent.r32.sleeves.volatility_risk_regime`
EVENT_DRIVEN                 :mod:`alpha_agent.r32.sleeves.event_driven`
===========================  =================================================
"""
from __future__ import annotations

from . import (  # noqa: F401
    cross_asset_trend,
    equity_beta_timing,
    equity_selection,
    event_driven,
    sector_rotation,
    volatility_risk_regime,
)

MODULES = {
    "EQUITY_SELECTION": equity_selection,
    "EQUITY_BETA_TIMING": equity_beta_timing,
    "SECTOR_ROTATION": sector_rotation,
    "CROSS_ASSET_TREND": cross_asset_trend,
    "VOLATILITY_RISK_REGIME": volatility_risk_regime,
    "EVENT_DRIVEN": event_driven,
}
