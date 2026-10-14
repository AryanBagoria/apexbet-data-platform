-- Generic test: a monetary column must never be negative (minor units).
-- Reusable via {{ test_non_negative('column_name') }} or as a singular test.
{% test non_negative(model, column_name) %}
    select *
    from {{ model }}
    where {{ column_name }} < 0
{% endtest %}
