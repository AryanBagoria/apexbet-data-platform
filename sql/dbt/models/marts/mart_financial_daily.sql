-- mart_financial_daily: money movement per day / currency (minor units).
{{ config(materialized='table') }}

select
    event_date,
    currency,
    settled_stake_minor,
    payout_minor,
    deposits_minor,
    withdrawals_minor,
    gross_margin_minor,
    deposits_minor - withdrawals_minor as net_deposits_minor
from {{ ref('int_daily_financials') }}
