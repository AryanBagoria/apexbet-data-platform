-- int_daily_financials: daily roll-up of money movement (stakes, payouts,
-- deposits, withdrawals) in integer minor units. Feeds the finance mart.
{{ config(materialized='view') }}

with settled as (
    select
        event_date,
        currency,
        sum(settled_stake_minor) as settled_stake_minor,
        sum(payout_minor)        as payout_minor,
        0                        as deposits_minor,
        0                        as withdrawals_minor
    from {{ ref('stg_silver_events') }}
    where event_type = 'BetSettled'
    group by 1, 2
),

payments as (
    select
        event_date,
        currency,
        sum(case when event_type = 'DepositRecorded'    then amount_minor else 0 end) as deposits_minor,
        sum(case when event_type = 'WithdrawalRecorded' then amount_minor else 0 end) as withdrawals_minor,
        0 as settled_stake_minor,
        0 as payout_minor
    from {{ ref('stg_silver_events') }}
    where event_type in ('DepositRecorded', 'WithdrawalRecorded')
    group by 1, 2
),

unioned as (
    select * from settled
    union all
    select * from payments
)

select
    event_date,
    currency,
    sum(settled_stake_minor) as settled_stake_minor,
    sum(payout_minor)        as payout_minor,
    sum(deposits_minor)      as deposits_minor,
    sum(withdrawals_minor)   as withdrawals_minor,
    sum(payout_minor) - sum(settled_stake_minor) as gross_margin_minor
from unioned
group by 1, 2
