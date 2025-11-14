{% macro clean_unknown(expr) -%}
    CASE
        WHEN {{ expr }} IS NULL THEN NULL
        WHEN LOWER({{ expr }}) IN ('unknown', 'n/a', 'na', '') THEN NULL
        ELSE {{ expr }}
    END
{%- endmacro %}
