"""ApexBet event models: a versioned envelope plus typed payloads.

Design principles
-----------------
* A *single* common envelope wraps every event type so downstream code can
  route, dedupe and partition generically.
* Payloads are strongly typed per event type and validated with Pydantic v2.
* Schema versions are explicit (``schema_version``). The current version is
  ``SCHEMA_VERSION_CURRENT``; anything below ``SCHEMA_VERSION_MIN_SUPPORTED``
  is rejected as unsupported.
* Monetary values are integer minor units (see ``common.money``).
* All timestamps are UTC ISO-8601 strings.

This module is the single source of truth for event contracts. JSON Schema
for documentation is generated from these models (``export_json_schemas``).
"""

from __future__ import annotations

import json
from enum import Enum
from typing import Annotated, Any, Literal, Union

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from apexbet import SCHEMA_VERSION_CURRENT, SCHEMA_VERSION_MIN_SUPPORTED
from apexbet.common.ids import new_event_id
from apexbet.common.timeutil import parse_iso

# --------------------------------------------------------------------------
# Enumerations
# --------------------------------------------------------------------------


class EventType(str, Enum):
    BET_PLACED = "BetPlaced"
    BET_ACCEPTED = "BetAccepted"
    BET_REJECTED = "BetRejected"
    BET_SETTLED = "BetSettled"
    ODDS_UPDATED = "OddsUpdated"
    MARKET_OPENED = "MarketOpened"
    MARKET_SUSPENDED = "MarketSuspended"
    GAME_SESSION_STARTED = "GameSessionStarted"
    GAME_SESSION_ENDED = "GameSessionEnded"
    DEPOSIT_RECORDED = "DepositRecorded"
    WITHDRAWAL_RECORDED = "WithdrawalRecorded"
    ACCOUNT_STATUS_CHANGED = "AccountStatusChanged"
    RESPONSIBLE_GAMBLING_LIMIT_CHANGED = "ResponsibleGamblingLimitChanged"
    RISK_SIGNAL_GENERATED = "RiskSignalGenerated"
    PIPELINE_HEALTH_EVENT = "PipelineHealthEvent"


class BetStatus(str, Enum):
    PLACED = "PLACED"
    ACCEPTED = "ACCEPTED"
    REJECTED = "REJECTED"
    SETTLED = "SETTLED"
    CANCELLED = "CANCELLED"


class SettlementResult(str, Enum):
    WON = "WON"
    LOST = "LOST"
    VOID = "VOID"
    PUSH = "PUSH"
    CASH_OUT = "CASH_OUT"


class AccountStatus(str, Enum):
    ACTIVE = "ACTIVE"
    SUSPENDED = "SUSPENDED"
    DORMANT = "DORMANT"
    SELF_EXCLUDED = "SELF_EXCLUDED"
    CLOSED = "CLOSED"


class RejectionReason(str, Enum):
    INSUFFICIENT_FUNDS = "INSUFFICIENT_FUNDS"
    MARKET_SUSPENDED = "MARKET_SUSPENDED"
    ODDS_CHANGED = "ODDS_CHANGED"
    ACCOUNT_RESTRICTED = "ACCOUNT_RESTRICTED"
    LIMIT_EXCEEDED = "LIMIT_EXCEEDED"
    INVALID_MARKET = "INVALID_MARKET"
    DUPLICATE_BET = "DUPLICATE_BET"


# --------------------------------------------------------------------------
# Common envelope
# --------------------------------------------------------------------------


class EventEnvelope(BaseModel):
    """The common, transport-agnostic event envelope."""

    model_config = ConfigDict(extra="forbid", use_enum_values=True)

    event_id: str = Field(default_factory=new_event_id, min_length=3, max_length=64)
    event_type: EventType
    schema_version: str = SCHEMA_VERSION_CURRENT
    event_timestamp: str = Field(description="UTC ISO-8601 time the event occurred")
    ingestion_timestamp: str | None = Field(
        default=None, description="UTC ISO-8601 time the platform accepted the event"
    )
    producer: str = Field(default="synthetic-generator", max_length=64)
    account_id: str | None = Field(default=None, max_length=64)
    session_id: str | None = Field(default=None, max_length=64)
    correlation_id: str | None = Field(default=None, max_length=64)
    partition_key: str | None = Field(
        default=None, description="Routing key; equals account_id when available"
    )
    trace_id: str | None = Field(default=None, max_length=64)
    payload: dict[str, Any] = Field(default_factory=dict)

    @field_validator("event_timestamp", "ingestion_timestamp")
    @classmethod
    def _validate_ts(cls, v: str | None) -> str | None:
        if v is None:
            return v
        parse_iso(v)  # raises ValueError on malformed input
        return v

    @model_validator(mode="after")
    def _default_partition_key(self) -> EventEnvelope:
        if not self.partition_key:
            self.partition_key = self.account_id or self.event_id
        return self


def is_supported_version(version: str) -> bool:
    """Return True when a schema version is within [min_supported, current]."""

    def _t(v: str) -> tuple[int, ...]:
        try:
            return tuple(int(p) for p in v.split("."))
        except ValueError:  # pragma: no cover - defensive
            return (0,)

    return _t(SCHEMA_VERSION_MIN_SUPPORTED) <= _t(version) <= _t(SCHEMA_VERSION_CURRENT)


# --------------------------------------------------------------------------
# Payload models (one per event type)
# --------------------------------------------------------------------------


class _Strict(BaseModel):
    model_config = ConfigDict(extra="allow")


class BetPlacedPayload(_Strict):
    bet_id: str
    market_id: str
    outcome_id: str
    stake_minor: int = Field(ge=0)
    currency: str = Field(min_length=3, max_length=3)
    decimal_odds: float = Field(gt=1.0)
    potential_return_minor: int = Field(ge=0)
    bet_status: BetStatus = BetStatus.PLACED
    channel: str = "mobile_web"
    sport_code: str | None = None


class BetAcceptedPayload(_Strict):
    bet_id: str
    market_id: str
    accepted_odds: float = Field(gt=1.0)
    accepted_stake_minor: int = Field(ge=0)
    currency: str = Field(min_length=3, max_length=3)
    liability_minor: int = Field(ge=0)


class BetRejectedPayload(_Strict):
    bet_id: str
    market_id: str | None = None
    reason: RejectionReason
    rejected_stake_minor: int = Field(ge=0)
    currency: str = Field(min_length=3, max_length=3)


class BetSettledPayload(_Strict):
    bet_id: str
    market_id: str
    outcome_id: str
    result: SettlementResult
    settled_stake_minor: int = Field(ge=0)
    payout_minor: int = Field(ge=0)
    currency: str = Field(min_length=3, max_length=3)
    ledger_entry_id: str | None = None


class OddsUpdatedPayload(_Strict):
    market_id: str
    outcome_id: str
    previous_odds: float = Field(gt=1.0)
    new_odds: float = Field(gt=1.0)
    movement_direction: Literal["UP", "DOWN", "UNCHANGED"] = "UNCHANGED"
    sequence: int = 0


class MarketOpenedPayload(_Strict):
    market_id: str
    match_id: str | None = None
    sport_code: str
    market_type: str
    outcome_ids: list[str] = Field(default_factory=list)


class MarketSuspendedPayload(_Strict):
    market_id: str
    reason: str
    resume_expected: bool = False


class GameSessionStartedPayload(_Strict):
    game_type: Literal["SLOTS", "ROULETTE", "BLACKJACK", "BACCARAT", "POKER", "CRASH"]
    game_id: str
    starting_balance_minor: int = Field(ge=0)
    currency: str = Field(min_length=3, max_length=3)
    device: str = "web"


class GameSessionEndedPayload(_Strict):
    game_type: str
    game_id: str
    duration_seconds: int = Field(ge=0)
    total_wagered_minor: int = Field(ge=0)
    total_won_minor: int = Field(ge=0)
    ending_balance_minor: int = Field(ge=0)
    currency: str = Field(min_length=3, max_length=3)


class DepositRecordedPayload(_Strict):
    transaction_id: str
    amount_minor: int = Field(ge=0)
    currency: str = Field(min_length=3, max_length=3)
    method: Literal["CARD", "BANK_TRANSFER", "EWALLET", "CRYPTO", "VOUCHER"] = "CARD"
    ledger_entry_id: str | None = None


class WithdrawalRecordedPayload(_Strict):
    transaction_id: str
    amount_minor: int = Field(ge=0)
    currency: str = Field(min_length=3, max_length=3)
    method: Literal["CARD", "BANK_TRANSFER", "EWALLET", "CRYPTO", "VOUCHER"] = "CARD"
    ledger_entry_id: str | None = None


class AccountStatusChangedPayload(_Strict):
    previous_status: AccountStatus
    new_status: AccountStatus
    reason: str
    changed_by: str = "system"


class ResponsibleGamblingLimitChangedPayload(_Strict):
    limit_type: Literal["DEPOSIT", "LOSS", "STAKE", "SESSION_TIME"]
    previous_limit_minor: int | None = None
    new_limit_minor: int | None = None
    period: Literal["DAILY", "WEEKLY", "MONTHLY"] = "DAILY"
    currency: str = Field(default="USD", min_length=3, max_length=3)


class RiskSignalGeneratedPayload(_Strict):
    signal_type: str
    severity: Literal["LOW", "MEDIUM", "HIGH", "CRITICAL"] = "LOW"
    score: float = Field(ge=0.0, le=1.0)
    evidence: dict[str, Any] = Field(default_factory=dict)


class PipelineHealthEventPayload(_Strict):
    component: str
    status: Literal["OK", "DEGRADED", "FAILED"]
    detail: str = ""
    metrics: dict[str, Any] = Field(default_factory=dict)


# Maps event type -> payload model for validation dispatch.
PAYLOAD_MODELS: dict[EventType, type[BaseModel]] = {
    EventType.BET_PLACED: BetPlacedPayload,
    EventType.BET_ACCEPTED: BetAcceptedPayload,
    EventType.BET_REJECTED: BetRejectedPayload,
    EventType.BET_SETTLED: BetSettledPayload,
    EventType.ODDS_UPDATED: OddsUpdatedPayload,
    EventType.MARKET_OPENED: MarketOpenedPayload,
    EventType.MARKET_SUSPENDED: MarketSuspendedPayload,
    EventType.GAME_SESSION_STARTED: GameSessionStartedPayload,
    EventType.GAME_SESSION_ENDED: GameSessionEndedPayload,
    EventType.DEPOSIT_RECORDED: DepositRecordedPayload,
    EventType.WITHDRAWAL_RECORDED: WithdrawalRecordedPayload,
    EventType.ACCOUNT_STATUS_CHANGED: AccountStatusChangedPayload,
    EventType.RESPONSIBLE_GAMBLING_LIMIT_CHANGED: ResponsibleGamblingLimitChangedPayload,
    EventType.RISK_SIGNAL_GENERATED: RiskSignalGeneratedPayload,
    EventType.PIPELINE_HEALTH_EVENT: PipelineHealthEventPayload,
}


class ValidationIssue(BaseModel):
    code: str
    message: str
    field: str | None = None


class ValidationResult(BaseModel):
    """Outcome of validating a raw dict as an envelope + typed payload."""

    ok: bool
    envelope: EventEnvelope | None = None
    issues: list[ValidationIssue] = Field(default_factory=list)


def validate_event(raw: dict[str, Any]) -> ValidationResult:
    """Validate a raw dict as an ApexBet event.

    Returns a structured result rather than raising so callers can route
    invalid records to quarantine while keeping the pipeline alive.
    """
    issues: list[ValidationIssue] = []
    try:
        env = EventEnvelope.model_validate(raw)
    except Exception as exc:  # pydantic ValidationError
        for err in getattr(exc, "errors", lambda: [])():
            loc = ".".join(str(p) for p in err.get("loc", ()))
            issues.append(
                ValidationIssue(
                    code="envelope_invalid", message=err.get("msg", str(exc)), field=loc
                )
            )
        if not issues:
            issues.append(ValidationIssue(code="envelope_invalid", message=str(exc)))
        return ValidationResult(ok=False, issues=issues)

    if not is_supported_version(env.schema_version):
        issues.append(
            ValidationIssue(
                code="unsupported_schema_version",
                message=(
                    f"schema_version {env.schema_version} outside supported range "
                    f"[{SCHEMA_VERSION_MIN_SUPPORTED}, {SCHEMA_VERSION_CURRENT}]"
                ),
                field="schema_version",
            )
        )

    model = PAYLOAD_MODELS.get(EventType(env.event_type))
    if model is not None:
        try:
            model.model_validate(env.payload)
        except Exception as exc:
            for err in getattr(exc, "errors", lambda: [])():
                loc = ".".join(str(p) for p in err.get("loc", ()))
                issues.append(
                    ValidationIssue(
                        code="payload_invalid",
                        message=err.get("msg", str(exc)),
                        field=f"payload.{loc}" if loc else "payload",
                    )
                )

    return ValidationResult(ok=not issues, envelope=env, issues=issues)


UnionPayload = Union[  # noqa: UP007 - kept explicit for docs
    BetPlacedPayload,
    BetAcceptedPayload,
    BetRejectedPayload,
    BetSettledPayload,
    OddsUpdatedPayload,
    MarketOpenedPayload,
    MarketSuspendedPayload,
    GameSessionStartedPayload,
    GameSessionEndedPayload,
    DepositRecordedPayload,
    WithdrawalRecordedPayload,
    AccountStatusChangedPayload,
    ResponsibleGamblingLimitChangedPayload,
    RiskSignalGeneratedPayload,
    PipelineHealthEventPayload,
]

AnnotatedEnvelope = Annotated[EventEnvelope, Field(description="ApexBet common event envelope")]


def export_json_schemas(out_path: str) -> dict[str, Any]:
    """Write JSON Schemas for the envelope + every payload to ``out_path``.

    Returns the in-memory mapping (event_type -> schema dict).
    """
    schemas: dict[str, Any] = {"EventEnvelope": EventEnvelope.model_json_schema()}
    for etype, model in PAYLOAD_MODELS.items():
        schemas[f"{etype.value}Payload"] = model.model_json_schema()
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(schemas, fh, indent=2, sort_keys=True)
    return schemas
