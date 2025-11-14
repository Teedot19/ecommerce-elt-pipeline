{% macro null_if_bad_country(expr) -%}
    CASE
      WHEN {{ expr }} IS NULL THEN NULL
      WHEN {{ expr }} IN ('', 'UNKNOWN', 'ERROR') THEN NULL
      ELSE {{ expr }}
    END
{%- endmacro %}