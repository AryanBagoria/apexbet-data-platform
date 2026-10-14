-- stg_silver_events: thin, typed view over the pipeline's Silver table.
--
-- Staging models do *no business logic*; they standardise naming, cast types and
-- flatten the nested `payload` struct into stable columns for the marts.
{{ config(materialized='view') }}

select
    event_id,
    event_type,
    schema_version,
    event_timestamp,
    event_date,
    event_category,
    account_id,
    session_id,
    sport_code,
    league_code,
    is_late,
    is_winning_settlement,
    processing_latency_ms,
    -- flattened, typed monetary fields (integer minor units)
    cast(payload.stake_minor         as bigint) as stake_minor,
    cast(payload.accepted_stake_minor as bigint) as accepted_stake_minor,
    cast(payload.settled_stake_minor as bigint) as settled_stake_minor,
    cast(payload.payout_minor        as bigint) as payout_minor,
    cast(payload.amount_minor        as bigint) as amount_minor,
    cast(payload.total_wagered_minor as bigint) as total_wagered_minor,
    cast(payload.total_won_minor     as bigint) as total_won_minor,
    payload.currency                          as currency,
    payload.result                            as result,
    payload.bet_id                            as bet_id,
    payload.market_id                         as market_id,
    payload.game_type                         as game_type
from {{ source('apexbet_raw', 'silver_events') }}
