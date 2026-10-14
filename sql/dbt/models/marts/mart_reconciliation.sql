-- mart_reconciliation: financial reconciliation findings with a clear
-- pass/fail flag for dashboards and on-call triage.
{{ config(materialized='table') }}

select
    rule_id,
    description,
    severity,
    expected,
    actual,
    difference,
    resolution_status,
    case when severity = 'info' then true else false end as ok
from {{ ref('stg_reconciliation_results') }}
order by
    case severity
        when 'critical' then 0
        when 'error'    then 1
        when 'warning'  then 2
        else 3
    end,
    rule_id
