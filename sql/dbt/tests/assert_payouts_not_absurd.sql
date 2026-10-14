-- Singular test: settled stake total must equal payout total only for minor
-- differences; this flags a day where payouts exceed stakes by >5x (a signal of
-- a settlement bug or injected anomaly). Not a hard business rule -- a guard.
select *
from {{ ref('mart_financial_daily') }}
where payout_minor > settled_stake_minor * 5
  and settled_stake_minor > 0
