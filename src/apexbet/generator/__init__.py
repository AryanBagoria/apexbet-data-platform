"""Synthetic data generator package."""

from apexbet.generator.engine import (
    AnomalyInjection,
    GeneratorConfig,
    SyntheticEventGenerator,
    payout_minor,
)
from apexbet.generator.reference import (
    CASINO_GAME_TYPES,
    CURRENCIES,
    EVENT_SOURCE_SYSTEMS,
    LEAGUES,
    MARKET_TYPES,
    SPORTS,
    ReferenceData,
)

__all__ = [
    "CASINO_GAME_TYPES",
    "CURRENCIES",
    "EVENT_SOURCE_SYSTEMS",
    "LEAGUES",
    "MARKET_TYPES",
    "SPORTS",
    "AnomalyInjection",
    "GeneratorConfig",
    "ReferenceData",
    "SyntheticEventGenerator",
    "payout_minor",
]
