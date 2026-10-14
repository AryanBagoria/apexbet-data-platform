-- mart_casino_activity: casino session KPIs per day / game type.
{{ config(materialized='table') }}

select
    event_date,
    game_type,
    count(*)                                                as session_events,
    sum(case when event_type = 'GameSessionEnded' then 1 else 0 end) as sessions_ended,
    sum(case when event_type = 'GameSessionEnded' then total_wagered_minor else 0 end) as wagered_minor,
    sum(case when event_type = 'GameSessionEnded' then total_won_minor     else 0 end) as won_minor
from {{ ref('stg_silver_events') }}
where event_category = 'CASINO'
group by 1, 2
