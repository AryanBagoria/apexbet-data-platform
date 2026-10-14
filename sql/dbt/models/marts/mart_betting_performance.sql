-- mart_betting_performance: betting KPIs per day / sport.
-- Grain: event_date x sport_code. One row per placed bet flows through the
-- lifecycle spine, so counts are exact and stake/payout are integer minor units.
{{ config(materialized='table') }}

select
    event_date,
    sport_code,
    count(*)                                                    as bets,
    sum(stake_minor)                                            as staked_minor,
    sum(case when final_status = 'SETTLED'  then 1 else 0 end)  as settled_bets,
    sum(case when final_status = 'REJECTED' then 1 else 0 end)  as rejected_bets,
    sum(coalesce(settled_stake_minor, 0))                       as settled_stake_minor,
    sum(coalesce(payout_minor, 0))                              as payout_minor,
    sum(gross_margin_minor)                                     as gross_margin_minor,
    round(
        cast(sum(case when final_status = 'REJECTED' then 1 else 0 end) as double)
        / nullif(count(*), 0),
        4
    )                                                           as rejection_rate
from {{ ref('int_betting_lifecycle') }}
group by 1, 2
