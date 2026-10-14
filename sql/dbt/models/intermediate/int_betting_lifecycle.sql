-- int_betting_lifecycle: joins BetPlaced -> BetAccepted/Rejected -> BetSettled
-- per bet_id into a single lifecycle row. This is the canonical "one row per
-- bet" spine the Gold marts aggregate from.
{{ config(materialized='view') }}

with placed as (
    select bet_id, account_id, market_id, event_date, sport_code,
           stake_minor, currency, event_timestamp as placed_at
    from {{ ref('stg_silver_events') }}
    where event_type = 'BetPlaced'
),

accepted as (
    select bet_id,
           accepted_stake_minor,
           event_timestamp as accepted_at
    from {{ ref('stg_silver_events') }}
    where event_type = 'BetAccepted'
),

rejected as (
    select bet_id, event_timestamp as rejected_at
    from {{ ref('stg_silver_events') }}
    where event_type = 'BetRejected'
),

settled as (
    select bet_id, result, settled_stake_minor, payout_minor,
           event_timestamp as settled_at
    from {{ ref('stg_silver_events') }}
    where event_type = 'BetSettled'
)

select
    p.bet_id,
    p.account_id,
    p.market_id,
    p.event_date,
    p.sport_code,
    p.currency,
    p.stake_minor,
    p.placed_at,
    a.accepted_stake_minor,
    a.accepted_at,
    r.rejected_at,
    s.result,
    s.settled_stake_minor,
    s.payout_minor,
    s.settled_at,
    case
        when s.bet_id is not null then 'SETTLED'
        when r.bet_id is not null then 'REJECTED'
        when a.bet_id is not null then 'ACCEPTED'
        else 'PLACED'
    end as final_status,
    case
        when s.bet_id is not null
        then s.payout_minor - s.settled_stake_minor
        else 0
    end as gross_margin_minor
from placed p
left join accepted a on p.bet_id = a.bet_id
left join rejected r on p.bet_id = r.bet_id
left join settled  s on p.bet_id = s.bet_id
