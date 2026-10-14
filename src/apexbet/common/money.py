"""Money handling.

All monetary values are represented as integer *minor currency units*
(cents for USD/EUR/GBP) to guarantee exact, reproducible arithmetic with
no floating point drift. Decimals are only used at the boundary when
parsing/formatting.
"""

from __future__ import annotations

from decimal import ROUND_HALF_UP, Decimal, InvalidOperation

MINOR_UNITS_PER_MAJOR = 100
_DEFAULT_EXPONENT = Decimal("0.01")


class MoneyError(ValueError):
    """Raised for invalid monetary values."""


def to_minor(major: Decimal | str | float | int, exponent: Decimal = _DEFAULT_EXPONENT) -> int:
    """Convert a major-unit amount (e.g. 12.34) to integer minor units (1234).

    Uses ROUND_HALF_UP so 12.345 -> 1235 and 12.344 -> 1234, deterministically.
    """
    try:
        d = Decimal(str(major))
    except (InvalidOperation, ValueError) as exc:  # pragma: no cover - defensive
        raise MoneyError(f"not a valid decimal: {major!r}") from exc
    quantized = d.quantize(exponent, rounding=ROUND_HALF_UP)
    return int((quantized / exponent).to_integral_value(rounding=ROUND_HALF_UP))


def to_major(minor: int, exponent: Decimal = _DEFAULT_EXPONENT) -> Decimal:
    """Convert integer minor units back to a major-unit Decimal."""
    return (Decimal(minor) * exponent).quantize(exponent)


def format_money(minor: int, currency: str = "USD") -> str:
    return f"{to_major(minor)} {currency}"


def is_valid_amount(minor: int, *, allow_zero: bool = False, max_minor: int = 10**12) -> bool:
    """Validity rule for a monetary value in minor units.

    Rejects negatives, out-of-range magnitudes, and (optionally) zero.
    """
    if not isinstance(minor, int):
        return False
    if minor < 0:
        return False
    if not allow_zero and minor == 0:
        return False
    return minor <= max_minor


def apply_payout(stake_minor: int, decimal_odds: Decimal) -> int:
    """Return the gross return (stake * odds) in minor units.

    A stake of 10000 minor units (100.00) @ 2.50 returns 25000 minor units
    (250.00). Arithmetic is exact: both operands are integers / Decimals.
    """
    return int(
        (Decimal(stake_minor) * Decimal(str(decimal_odds))).to_integral_value(
            rounding=ROUND_HALF_UP
        )
    )
