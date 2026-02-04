{#
    Macro to generate vendor_name column using Jinja dictionary.

    This approach works seamlessly across BigQuery, DuckDB, Snowflake, etc.
    by generating a CASE statement at compile time.

    Usage: {{ get_vendor_data('vendor_id') }}
    Returns: SQL CASE expression that maps vendor_id to vendor_name
#}

{% macro get_vendor_data(vendor_id) %}
case
  when {{ vendor_id }} = 1 then 'Creative Mobile Technologies, LLC'
  when {{ vendor_id }} = 2 then 'VeriFone Inc.'
  when {{ vendor_id }} = 4 then 'Unknown vendor'
  else 'Other / Unknown'
end
{% endmacro %}