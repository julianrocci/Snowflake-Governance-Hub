{% macro get_domain_from_warehouse(column_name) %}
    CASE 
        WHEN {{ column_name }} LIKE 'FIN_%' THEN 'FINANCE'
        WHEN {{ column_name }} LIKE 'MKT_%' THEN 'MARKETING'
        WHEN {{ column_name }} LIKE 'ECO_%' THEN 'ECOMMERCE'
        WHEN {{ column_name }} LIKE 'RET_%' THEN 'RETAIL'
        WHEN {{ column_name }} LIKE 'ANA_%' THEN 'ANALYTICS'
        WHEN {{ column_name }} LIKE 'LOY_%' THEN 'LOYALTY'
        WHEN {{ column_name }} LIKE 'MGMT_%' THEN 'MANAGEMENT'
        WHEN {{ column_name }} LIKE 'TRANSFORM_%' THEN 'DATA_ENG'
        ELSE 'OTHER'
    END
{% endmacro %}

{% macro get_domain_from_database(column_name) %}
    CASE 
        WHEN {{ column_name }} LIKE 'FIN%' THEN 'FINANCE'
        WHEN {{ column_name }} LIKE 'MKT%' OR {{ column_name }} LIKE 'MARKET%' THEN 'MARKETING'
        WHEN {{ column_name }} LIKE 'ECO%' OR {{ column_name }} LIKE 'ECOMM%' THEN 'ECOMMERCE'
        WHEN {{ column_name }} LIKE 'RET%' THEN 'RETAIL'
        WHEN {{ column_name }} LIKE 'ANA%' THEN 'ANALYTICS'
        WHEN {{ column_name }} LIKE 'LOY%' THEN 'LOYALTY'
        WHEN {{ column_name }} LIKE 'MGMT%' THEN 'MANAGEMENT'
        ELSE 'OTHER'
    END
{% endmacro %}

{% macro get_environment_from_name(column_name) %}
    CASE 
        WHEN {{ column_name }} LIKE '%_PROD' OR {{ column_name }} LIKE '%PROD%' THEN 'PROD'
        WHEN {{ column_name }} LIKE '%_UAT' OR {{ column_name }} LIKE '%UAT%' THEN 'UAT'
        WHEN {{ column_name }} LIKE '%_DEV' OR {{ column_name }} LIKE '%DEV%' THEN 'DEV'
        ELSE 'DEV'
    END
{% endmacro %}