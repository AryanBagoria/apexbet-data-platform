-- stg_dq_results: one row per data-quality rule result.
{{ config(materialized='view') }}

select
    rule_id,
    dataset,
    severity,
    cast(record_count  as bigint) as record_count,
    cast(failure_count as bigint) as failure_count,
    description,
    passed
from {{ source('apexbet_raw', 'dq_results') }}
