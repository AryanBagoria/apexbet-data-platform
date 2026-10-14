-- stg_reconciliation_results: one row per reconciliation finding.
{{ config(materialized='view') }}

select
    rule_id,
    description,
    severity,
    expected,
    actual,
    difference,
    resolution_status
from {{ source('apexbet_raw', 'reconciliation_results') }}
