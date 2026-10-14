-- mart_data_quality: latest data-quality rule results, one row per rule.
{{ config(materialized='table') }}

select
    rule_id,
    dataset,
    severity,
    record_count,
    failure_count,
    description,
    passed,
    case
        when record_count = 0 then 0
        else round(cast(failure_count as double) / record_count, 4)
    end as failure_rate
from {{ ref('stg_dq_results') }}
order by passed asc, failure_count desc
