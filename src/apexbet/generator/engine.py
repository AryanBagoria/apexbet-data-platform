"""Synthetic gambling event generator.

Produces internally consistent, reproducible event streams for the fictional
operator ApexBet. Supports both "clean" and "dirty" modes: it can emit valid
events plus deliberate duplicates, late arrivals, out-of-order events, invalid
monetary values, schema-version drift, missing reference data and settlement
discrepancies so that downstream quality, dedup and reconciliation logic can
be exercised.

The generator is a *streaming* iterator: callers pull events one at a time
(or in batches) so millions of events can be produced without holding them in
memory. The `AnomalyInjection` config injects controlled traffic spikes,
rejection-rate spikes and suspicious transaction patterns for the anomaly
detector to find.

All data is fictional.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from apexbet import SCHEMA_VERSION_CURRENT
from apexbet.common.ids import deterministic_id, new_correlation_id, new_event_id, new_trace_id
from apexbet.common.money import to_minor
from apexbet.common.rng import rng_for
from apexbet.common.timeutil import to_iso, utcnow
from apexbet.generator.reference import (
    CASINO_GAME_TYPES,
    CHANNELS,
    DEVICES,
    ReferenceData,
)
from apexbet.schemas.events import EventType


@dataclass
class AnomalyInjection:
    """Controls deliberate anomalies for testing detection & reconciliation."""

    duplicate_rate: float = 0.01
    late_rate: float = 0.02
    out_of_order_rate: float = 0.01
    invalid_amount_rate: float = 0.005
    missing_reference_rate: float = 0.01
    schema_drift_rate: float = 0.003
    settlement_discrepancy_rate: float = 0.01
    traffic_spike: bool = False
    spike_at_fraction: float = 0.5
    spike_multiplier: float = 8.0
    rejection_spike: bool = False
    suspicious_transaction_pattern: bool = False

    def to_dict(self) -> dict[str, Any]:
        return dict(self.__dict__.items())

    @classmethod
    def clean(cls) -> AnomalyInjection:
        return cls(
            duplicate_rate=0.0,
            late_rate=0.0,
            out_of_order_rate=0.0,
            invalid_amount_rate=0.0,
            missing_reference_rate=0.0,
            schema_drift_rate=0.0,
            settlement_discrepancy_rate=0.0,
        )

    @classmethod
    def demo(cls) -> AnomalyInjection:
        """Dirty-by-design profile used by the end-to-end demo."""
        return cls(traffic_spike=True, rejection_spike=True, suspicious_transaction_pattern=True)


@dataclass
class GeneratorConfig:
    seed: int = 1337
    num_accounts: int = 2000
    num_matches: int = 120
    currency_weights: dict[str, float] = field(
        default_factory=lambda: {"USD": 0.6, "EUR": 0.2, "GBP": 0.12, "BRL": 0.08}
    )
    # Mean seconds between events at 1x rate; driving the base pacing.
    base_interval_seconds: float = 0.02
    start_time_iso: str | None = None
    anomalies: AnomalyInjection = field(default_factory=AnomalyInjection)

    def to_dict(self) -> dict[str, Any]:
        return {
            "seed": self.seed,
            "num_accounts": self.num_accounts,
            "num_matches": self.num_matches,
            "currency_weights": self.currency_weights,
            "base_interval_seconds": self.base_interval_seconds,
            "start_time_iso": self.start_time_iso,
            "anomalies": self.anomalies.to_dict(),
        }


@dataclass
class Account:
    account_id: str
    currency: str
    status: str
    risk_tier: str
    deposit_limit_minor: int | None
    created_index: int


class SyntheticEventGenerator:
    """Stateful generator with an internal entity model.

    State (accounts, matches, open bets) is built deterministically from the
    seed so the same config yields the same stream.
    """

    def __init__(self, config: GeneratorConfig | None = None) -> None:
        self.config = config or GeneratorConfig()
        self.ref = ReferenceData.build()
        self.rng = rng_for(self.config.seed, "master")
        self._start = (
            utcnow() if not self.config.start_time_iso else _parse(self.config.start_time_iso)
        )
        self._clock = self._start
        self._emitted = 0
        self.accounts: dict[str, Account] = {}
        self._open_bets: dict[str, dict[str, Any]] = {}
        self._placed_envelopes: dict[str, dict[str, Any]] = {}
        self._accepted_bets: dict[str, dict[str, Any]] = {}
        self._pending_burst = 0
        self._matches: list[dict[str, Any]] = []
        self._markets: list[dict[str, Any]] = []
        self._build_entities()
        # Deterministic auxiliary streams.
        self._dup_rng = rng_for(self.config.seed, "dup")
        self._late_rng = rng_for(self.config.seed, "late")
        self._anom_rng = rng_for(self.config.seed, "anom")

    # -- entity construction -------------------------------------------------

    def _build_entities(self) -> None:
        weights = list(self.config.currency_weights.items())
        curr_codes = [k for k, _ in weights]
        curr_w = [v for _, v in weights]
        statuses = ["ACTIVE"] * 90 + ["DORMANT"] * 6 + ["SUSPENDED"] * 3 + ["SELF_EXCLUDED"]
        risk_tiers = ["LOW"] * 70 + ["MEDIUM"] * 22 + ["HIGH"] * 7 + ["CRITICAL"]
        for i in range(self.config.num_accounts):
            aid = f"acct_{i:07d}"
            self.accounts[aid] = Account(
                account_id=aid,
                currency=self.rng.choices(curr_codes, weights=curr_w, k=1)[0],
                status=self.rng.choice(statuses),
                risk_tier=self.rng.choice(risk_tiers),
                deposit_limit_minor=(
                    to_minor(self.rng.choice([500, 1000, 2000, 5000]))
                    if self.rng.random() < 0.3
                    else None
                ),
                created_index=i,
            )
        # Matches across leagues.
        leagues = list(self.ref.leagues.values())
        for i in range(self.config.num_matches):
            league = self.rng.choice(leagues)
            teams = self.ref.teams_for_league(league.code)
            if len(teams) < 2:
                continue
            home, away = self.rng.sample(teams, 2)
            self._matches.append(
                {
                    "match_id": f"match_{league.code.lower()}_{i:05d}",
                    "league_code": league.code,
                    "sport_code": league.sport_code,
                    "home_team_id": home.team_id,
                    "away_team_id": away.team_id,
                }
            )
        # Markets for each match.
        mid = 0
        for match in self._matches:
            for mt in self.ref.market_types_for_sport(match["sport_code"]):
                for outcome in mt.outcomes:
                    self._markets.append(
                        {
                            "market_id": f"mkt_{match['match_id']}_{mt.code}_{outcome}".lower(),
                            "match_id": match["match_id"],
                            "sport_code": match["sport_code"],
                            "market_type": mt.code,
                            "outcome_id": outcome,
                            "base_odds": round(self.rng.uniform(1.2, 6.0), 2),
                        }
                    )
                    mid += 1

    # -- helpers -------------------------------------------------------------

    def _advance(self, seconds: float) -> None:
        from datetime import timedelta

        self._clock = self._clock + timedelta(seconds=seconds)

    def _maybe_delay(self, base: float = 0.0) -> float:
        """Interval between events with burstiness and optional spike."""
        # Bursty: lognormal-ish pacing around base with occasional lulls.
        pace = self.config.base_interval_seconds * self.rng.expovariate(1.0)
        if base:
            pace = base
        return max(0.0005, pace)

    def _pick_account(self) -> Account:
        return self.rng.choice(list(self.accounts.values()))

    def _pick_market(self) -> dict[str, Any]:
        return self.rng.choice(self._markets)

    def _envelope(
        self,
        event_type: EventType,
        payload: dict[str, Any],
        *,
        account_id: str | None,
        producer: str,
        schema_version: str = SCHEMA_VERSION_CURRENT,
        session_id: str | None = None,
        correlation_id: str | None = None,
        event_time: str | None = None,
    ) -> dict[str, Any]:
        et = event_time or to_iso(self._clock)
        return {
            "event_id": new_event_id(),
            "event_type": event_type.value,
            "schema_version": schema_version,
            "event_timestamp": et,
            "ingestion_timestamp": to_iso(utcnow()),
            "producer": producer,
            "account_id": account_id,
            "session_id": session_id,
            "correlation_id": correlation_id or new_correlation_id(),
            "partition_key": account_id or new_event_id(),
            "trace_id": new_trace_id(),
            "payload": payload,
        }

    # -- event builders ------------------------------------------------------

    def _bet_placed(self) -> dict[str, Any]:
        acct = self._pick_account()
        market = self._pick_market()
        stake = self.rng.choice([100, 200, 500, 1000, 2000, 5000, 10000, 25000])
        odds = round(max(1.01, market["base_odds"] * self.rng.uniform(0.9, 1.1)), 2)
        potential = to_minor(Decimal(stake) / 100 * Decimal(str(odds)))
        bet_id = deterministic_id("bet", acct.account_id, self._emitted)
        placed = self._envelope(
            EventType.BET_PLACED,
            {
                "bet_id": bet_id,
                "market_id": market["market_id"],
                "outcome_id": market["outcome_id"],
                "stake_minor": stake,
                "currency": acct.currency,
                "decimal_odds": odds,
                "potential_return_minor": potential,
                "bet_status": "PLACED",
                "channel": self.rng.choice(CHANNELS),
                "sport_code": market["sport_code"],
            },
            account_id=acct.account_id,
            producer="sportsbook-core",
        )
        self._open_bets[bet_id] = {
            "market": market,
            "odds": odds,
            "stake": stake,
            "currency": acct.currency,
            "account_id": acct.account_id,
        }
        return placed

    def _bet_outcome(self, placed: dict[str, Any]) -> dict[str, Any] | None:
        """Given an open BetPlaced envelope, produce Accepted or Rejected."""
        payload = placed["payload"]
        bet_id = payload["bet_id"]
        info = self._open_bets.pop(bet_id, None)
        if info is None:
            return None
        acct = self.accounts[info["account_id"]]
        reject_prob = 0.12 if acct.status in ("SUSPENDED", "SELF_EXCLUDED") else 0.05
        if self.config.anomalies.rejection_spike and self._anom_rng.random() < 0.5:
            reject_prob = 0.6
        if self.rng.random() < reject_prob:
            reason = self.rng.choice(
                [
                    "INSUFFICIENT_FUNDS",
                    "MARKET_SUSPENDED",
                    "ODDS_CHANGED",
                    "ACCOUNT_RESTRICTED",
                    "LIMIT_EXCEEDED",
                    "INVALID_MARKET",
                ]
            )
            return self._envelope(
                EventType.BET_REJECTED,
                {
                    "bet_id": bet_id,
                    "market_id": payload["market_id"],
                    "reason": reason,
                    "rejected_stake_minor": payload["stake_minor"],
                    "currency": payload["currency"],
                },
                account_id=info["account_id"],
                producer="sportsbook-core",
                correlation_id=placed["correlation_id"],
            )
        accepted_odds = round(max(1.01, info["odds"] * self.rng.uniform(0.97, 1.03)), 2)
        self._accepted_bets[bet_id] = {**info, "accepted_odds": accepted_odds}
        return self._envelope(
            EventType.BET_ACCEPTED,
            {
                "bet_id": bet_id,
                "market_id": payload["market_id"],
                "accepted_odds": accepted_odds,
                "accepted_stake_minor": payload["stake_minor"],
                "currency": payload["currency"],
                "liability_minor": to_minor(
                    Decimal(payload["stake_minor"]) / 100 * Decimal(str(accepted_odds - 1))
                ),
            },
            account_id=info["account_id"],
            producer="sportsbook-core",
            correlation_id=placed["correlation_id"],
        )

    def _bet_settled(self) -> dict[str, Any]:
        bet_id = self.rng.choice(list(self._accepted_bets.keys()))
        info = self._accepted_bets.pop(bet_id)
        result = self.rng.choices(
            ["WON", "LOST", "VOID", "PUSH", "CASH_OUT"], weights=[30, 60, 3, 4, 3], k=1
        )[0]
        odds = info["accepted_odds"]
        stake = info["stake"]
        if result == "WON":
            payout = payout_minor(stake, odds)
        elif result in ("VOID", "PUSH"):
            payout = stake
        elif result == "CASH_OUT":
            payout = int(stake * self.rng.uniform(0.5, 1.4))
        else:
            payout = 0
        # Inject a deliberate settlement discrepancy (payout off by a factor).
        if self.rng.random() < self.config.anomalies.settlement_discrepancy_rate:
            payout = int(payout * self.rng.choice([0.5, 1.5, 2.0]))
        market = info["market"]
        return self._envelope(
            EventType.BET_SETTLED,
            {
                "bet_id": bet_id,
                "market_id": market["market_id"],
                "outcome_id": market["outcome_id"],
                "result": result,
                "settled_stake_minor": stake,
                "payout_minor": payout,
                "currency": info["currency"],
                "ledger_entry_id": deterministic_id("ledger", bet_id, result),
            },
            account_id=info["account_id"],
            producer="sportsbook-core",
        )

    def _odds_updated(self) -> dict[str, Any]:
        market = self._pick_market()
        prev = market["base_odds"]
        new = round(max(1.01, prev * self.rng.uniform(0.85, 1.15)), 2)
        market["base_odds"] = new
        direction = "UP" if new > prev else ("DOWN" if new < prev else "UNCHANGED")
        return self._envelope(
            EventType.ODDS_UPDATED,
            {
                "market_id": market["market_id"],
                "outcome_id": market["outcome_id"],
                "previous_odds": prev,
                "new_odds": new,
                "movement_direction": direction,
                "sequence": self._emitted,
            },
            account_id=None,
            producer="odds-feed-bridge",
        )

    def _casino_start(self) -> dict[str, Any]:
        acct = self._pick_account()
        game_type = self.rng.choice(CASINO_GAME_TYPES)
        session_id = deterministic_id("sess", acct.account_id, self._emitted)
        self._sessions_open = getattr(self, "_sessions_open", {})
        self._sessions_open[session_id] = {
            "account_id": acct.account_id,
            "currency": acct.currency,
            "game_type": game_type,
            "started": self._clock,
        }
        return self._envelope(
            EventType.GAME_SESSION_STARTED,
            {
                "game_type": game_type,
                "game_id": f"game_{game_type.lower()}_{self.rng.randint(1, 40):03d}",
                "starting_balance_minor": self.rng.randint(500, 200_000),
                "currency": acct.currency,
                "device": self.rng.choice(DEVICES),
            },
            account_id=acct.account_id,
            producer="casino-engine",
            session_id=session_id,
        )

    def _casino_end(self) -> dict[str, Any] | None:
        sessions = getattr(self, "_sessions_open", {})
        if not sessions:
            return None
        session_id = self.rng.choice(list(sessions.keys()))
        info = sessions.pop(session_id)
        wagered = self.rng.randint(100, 50_000)
        # House edge ~ keeps won slightly below wagered on average.
        won = int(wagered * self.rng.uniform(0.0, 1.6))
        return self._envelope(
            EventType.GAME_SESSION_ENDED,
            {
                "game_type": info["game_type"],
                "game_id": f"game_{info['game_type'].lower()}_{self.rng.randint(1, 40):03d}",
                "duration_seconds": self.rng.randint(30, 3600),
                "total_wagered_minor": wagered,
                "total_won_minor": won,
                "ending_balance_minor": self.rng.randint(0, 300_000),
                "currency": info["currency"],
            },
            account_id=info["account_id"],
            producer="casino-engine",
            session_id=session_id,
        )

    def _payment(self, kind: str) -> dict[str, Any]:
        acct = self._pick_account()
        amount = self.rng.choice([1000, 2500, 5000, 10000, 20000, 50000, 100000])
        # Suspicious pattern: bursts of round-number deposits just under limits.
        if self.config.anomalies.suspicious_transaction_pattern and self._anom_rng.random() < 0.3:
            amount = self.rng.choice([49900, 49999, 49800, 49950])
        etype = EventType.DEPOSIT_RECORDED if kind == "deposit" else EventType.WITHDRAWAL_RECORDED
        return self._envelope(
            etype,
            {
                "transaction_id": deterministic_id("txn", kind, acct.account_id, self._emitted),
                "amount_minor": amount,
                "currency": acct.currency,
                "method": self.rng.choice(
                    ["CARD", "BANK_TRANSFER", "EWALLET", "CRYPTO", "VOUCHER"]
                ),
                "ledger_entry_id": deterministic_id("ledger", kind, self._emitted),
            },
            account_id=acct.account_id,
            producer="payments-gateway",
        )

    def _risk_signal(self) -> dict[str, Any]:
        acct = self.rng.choice(
            [a for a in self.accounts.values() if a.risk_tier in ("HIGH", "CRITICAL")]
            or list(self.accounts.values())
        )
        score = round(self.rng.uniform(0.5, 0.99), 3)
        return self._envelope(
            EventType.RISK_SIGNAL_GENERATED,
            {
                "signal_type": self.rng.choice(
                    ["RAPID_DEPOSITS", "ODDS_ARBITRAGE_HINT", "VELOCITY", "DEVICE_SHARING"]
                ),
                "severity": "HIGH" if score > 0.8 else "MEDIUM",
                "score": score,
                "evidence": {"window_seconds": 300, "event_count": self.rng.randint(5, 40)},
            },
            account_id=acct.account_id,
            producer="risk-engine",
        )

    def _rg_limit(self) -> dict[str, Any]:
        acct = self._pick_account()
        return self._envelope(
            EventType.RESPONSIBLE_GAMBLING_LIMIT_CHANGED,
            {
                "limit_type": self.rng.choice(["DEPOSIT", "LOSS", "STAKE", "SESSION_TIME"]),
                "previous_limit_minor": self.rng.choice([None, 10000, 20000]),
                "new_limit_minor": self.rng.choice([5000, 10000, 20000, 50000]),
                "period": self.rng.choice(["DAILY", "WEEKLY", "MONTHLY"]),
                "currency": acct.currency,
            },
            account_id=acct.account_id,
            producer="account-service",
        )

    def _account_status(self) -> dict[str, Any]:
        acct = self._pick_account()
        new_status = self.rng.choice(["ACTIVE", "SUSPENDED", "DORMANT", "SELF_EXCLUDED", "CLOSED"])
        prev = acct.status
        acct.status = new_status
        return self._envelope(
            EventType.ACCOUNT_STATUS_CHANGED,
            {
                "previous_status": prev,
                "new_status": new_status,
                "reason": self.rng.choice(
                    ["USER_REQUEST", "COMPLIANCE_REVIEW", "AML_FLAG", "INACTIVITY", "REACTIVATION"]
                ),
                "changed_by": self.rng.choice(["system", "agent_12", "agent_88"]),
            },
            account_id=acct.account_id,
            producer="account-service",
        )

    # -- core stream ---------------------------------------------------------

    def _market_opened(self) -> dict[str, Any]:
        match = self.rng.choice(self._matches)
        mt = self.rng.choice(self.ref.market_types_for_sport(match["sport_code"]))
        return self._envelope(
            EventType.MARKET_OPENED,
            {
                "market_id": f"mkt_{match['match_id']}_{mt.code}_new".lower(),
                "match_id": match["match_id"],
                "sport_code": match["sport_code"],
                "market_type": mt.code,
                "outcome_ids": list(mt.outcomes),
            },
            account_id=None,
            producer="odds-feed-bridge",
        )

    def _market_suspended(self) -> dict[str, Any]:
        market = self._pick_market()
        return self._envelope(
            EventType.MARKET_SUSPENDED,
            {
                "market_id": market["market_id"],
                "reason": self.rng.choice(
                    ["GOAL_SCORED", "IN_PLAY_RECALC", "FEED_DELAY", "INTEGRITY_REVIEW"]
                ),
                "resume_expected": self.rng.random() < 0.8,
            },
            account_id=None,
            producer="odds-feed-bridge",
        )

    def _pipeline_health(self) -> dict[str, Any]:
        return self._envelope(
            EventType.PIPELINE_HEALTH_EVENT,
            {
                "component": self.rng.choice(
                    ["ingestion-api", "bronze-writer", "silver-processor", "enrichment", "consumer"]
                ),
                "status": self.rng.choices(["OK", "DEGRADED", "FAILED"], weights=[85, 12, 3], k=1)[
                    0
                ],
                "detail": "synthetic heartbeat",
                "metrics": {"throughput_eps": self.rng.randint(100, 5000)},
            },
            account_id=None,
            producer="pipeline-monitor",
        )

    def _one_core_event(self) -> dict[str, Any]:
        """Pick and build a single 'core' event using weighted choices."""
        r = self.rng.random()
        if r < 0.24:
            return self._bet_placed()
        if r < 0.40 and self._open_bets:
            env = self.rng.choice(list(self._placed_envelopes.values()))
            return self._bet_outcome(env) or self._odds_updated()
        if r < 0.52 and self._accepted_bets:
            return self._bet_settled()
        if r < 0.63:
            return self._odds_updated()
        if r < 0.68:
            return self._casino_start()
        if r < 0.73:
            return self._casino_end() or self._odds_updated()
        if r < 0.79:
            return self._payment("deposit")
        if r < 0.83:
            return self._payment("withdrawal")
        if r < 0.86:
            return self._risk_signal()
        if r < 0.89:
            return self._rg_limit()
        if r < 0.92:
            return self._account_status()
        if r < 0.95:
            return self._market_opened()
        if r < 0.98:
            return self._market_suspended()
        return self._pipeline_health()

    def _maybe_emit_duplicate(self, env: dict[str, Any]) -> dict[str, Any] | None:
        """With the configured probability, return an exact duplicate of ``env``.

        Duplicates share the same ``event_id`` but arrive with a new ingestion
        timestamp, which is exactly the shape real at-least-once transports
        produce. Downstream dedup must collapse them.
        """
        if self._dup_rng.random() < self.config.anomalies.duplicate_rate:
            dup = dict(env)
            dup["payload"] = dict(env["payload"])
            dup["ingestion_timestamp"] = to_iso(utcnow())
            return dup
        return None

    def _remember_placed(self, env: dict[str, Any]) -> None:
        self._placed_envelopes[env["payload"]["bet_id"]] = env

    def generate_one(self) -> dict[str, Any]:
        """Produce the next event, applying the configured anomaly injection."""
        self._advance(self._maybe_delay())
        env = self._one_core_event()
        if env["event_type"] == EventType.BET_PLACED.value:
            self._remember_placed(env)

        anomalies = self.config.anomalies

        # Traffic spike: multiply effective event rate by emitting extra events.
        if anomalies.traffic_spike and self._should_spike():
            bursts = int(self.rng.uniform(3, anomalies.spike_multiplier))
            # Return the primary now; caller streams bursts via generate_burst.
            self._pending_burst = bursts

        # Schema drift: occasionally emit an unsupported/older schema version.
        if self.rng.random() < anomalies.schema_drift_rate:
            env["schema_version"] = self.rng.choice(["0.9.0", "1.0.0", "2.0.0"])

        # Missing reference data: strip a referenced id.
        if self.rng.random() < anomalies.missing_reference_rate:
            if "market_id" in env["payload"]:
                env["payload"]["market_id"] = "mkt_UNKNOWN_000"
            if env["account_id"] is None:
                env["account_id"] = "acct_MISSING"
                env["partition_key"] = "acct_MISSING"

        # Invalid monetary values.
        if self.rng.random() < anomalies.invalid_amount_rate:
            for key in ("stake_minor", "amount_minor", "payout_minor", "accepted_stake_minor"):
                if key in env["payload"]:
                    env["payload"][key] = self.rng.choice([-100, -1, 0])
                    break

        # Late arrival: backdate the event timestamp well into the past.
        if self.rng.random() < anomalies.late_rate:
            from datetime import timedelta

            skew = timedelta(seconds=self.rng.uniform(700, 3000))
            env["event_timestamp"] = to_iso(self._clock - skew)

        # Out-of-order: emit with an event_time slightly before the last one.
        if self.rng.random() < anomalies.out_of_order_rate:
            from datetime import timedelta

            skewed = self._clock - timedelta(seconds=self.rng.uniform(1, 60))
            env["event_timestamp"] = to_iso(skewed)

        self._emitted += 1
        return env

    def _should_spike(self) -> bool:
        if not self.config.anomalies.traffic_spike:
            return False
        # Simple deterministic spike around the configured fraction of the run.
        # During streaming we approximate by random bursts.
        return self._anom_rng.random() < 0.05

    def generate(self, count: int | None = None) -> Iterator[dict[str, Any]]:
        """Yield events. If ``count`` is None, yields forever.

        Anomaly injection (duplicates, bursts, late/out-of-order) is applied
        here so every consumer of the generator sees the same dirty stream.
        """
        produced = 0
        while count is None or produced < count:
            env = self.generate_one()
            yield env
            produced += 1

            dup = self._maybe_emit_duplicate(env)
            if dup is not None:
                yield dup
                produced += 1

            burst = getattr(self, "_pending_burst", 0)
            if burst:
                self._pending_burst = 0
                for _ in range(burst - 1):
                    if count is not None and produced >= count:
                        break
                    yield self.generate_one()
                    produced += 1

    def generate_batch(self, size: int) -> list[dict[str, Any]]:
        """Return exactly ``size`` events (anomalies included)."""
        out: list[dict[str, Any]] = []
        for env in self.generate(size):
            out.append(env)
        return out


def payout_minor(stake_minor: int, decimal_odds: float) -> int:
    return to_minor(Decimal(stake_minor) / 100 * Decimal(str(decimal_odds)))


def _parse(value: str):
    from apexbet.common.timeutil import parse_iso

    return parse_iso(value)
