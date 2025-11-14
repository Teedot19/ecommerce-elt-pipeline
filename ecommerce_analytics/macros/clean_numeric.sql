{% macro clean_numeric(expr) -%}
    /* Business-safe numeric: invalid OR negative -> 0 */
    CASE
      WHEN TRY_TO_NUMBER({{ expr }}) IS NULL THEN 0
      WHEN TRY_TO_NUMBER({{ expr }}) < 0 THEN 0
      ELSE TRY_TO_NUMBER({{ expr }})
    END
{%- endmacro %}
