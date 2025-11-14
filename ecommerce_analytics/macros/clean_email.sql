{% macro clean_email(expr) -%}
    /* Validates a basic email format; invalid -> NULL (None) */
    CASE
      WHEN {{ expr }} IS NULL THEN NULL
      WHEN REGEXP_LIKE({{ expr }}, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$')
        THEN LOWER({{ expr }})
      ELSE NULL
    END
{%- endmacro %}
