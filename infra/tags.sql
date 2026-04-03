/* Governance: Centralized tagging system for cost attribution. */

USE ROLE DCM_ADMIN;

-- GOVERNANCE TAGS CREATION

CREATE TAG IF NOT EXISTS {{ gov_db }}.CORE.OWNER
    ALLOWED_VALUES ('DATA_TEAM', 'FINANCE_TEAM', 'MARKETING_TEAM', 'ECOMMERCE_TEAM', 'RETAIL_TEAM', 'ANALYTICS_TEAM')
    COMMENT = 'Responsible team for the object lifecycle';

CREATE TAG IF NOT EXISTS {{ gov_db }}.CORE.DOMAIN
    ALLOWED_VALUES ('FINANCE', 'MARKETING', 'ECOMMERCE', 'RETAIL', 'ANALYTICS')
    COMMENT = 'Business domain logical mapping';

CREATE TAG IF NOT EXISTS {{ gov_db }}.CORE.ENVIRONMENT
    ALLOWED_VALUES ('DEV', 'PROD')
    COMMENT = 'Lifecycle environment tag';

CREATE TAG IF NOT EXISTS {{ gov_db }}.CORE.COST_CENTER
    ALLOWED_VALUES (
        'FIN_01', 'FIN_02', 
        'MKT_01', 'MKT_02', 
        'ECO_01', 'ECO_02', 
        'RET_01', 'RET_02', 
        'ANA_01', 'ANA_02'
    )
    COMMENT = 'Cost center by team for internal rebilling';


-- TAG ASSIGNMENT ON DATABASES

ALTER DATABASE {{ gov_db }} 
    SET TAG {{ gov_db }}.CORE.ENVIRONMENT = '{{ env_suffix | replace("_", "") }}',
            {{ gov_db }}.CORE.OWNER = 'DATA_TEAM',
            {{ gov_db }}.CORE.DOMAIN = 'ANALYTICS',
            {{ gov_db }}.CORE.COST_CENTER = 'ANA_01';

ALTER DATABASE {{ fin_db }} 
    SET TAG {{ gov_db }}.CORE.ENVIRONMENT = '{{ env_suffix | replace("_", "") }}',
            {{ gov_db }}.CORE.OWNER = 'FINANCE_TEAM',
            {{ gov_db }}.CORE.DOMAIN = 'FINANCE',
            {{ gov_db }}.CORE.COST_CENTER = 'FIN_01';

ALTER DATABASE {{ mkt_db }} 
    SET TAG {{ gov_db }}.CORE.ENVIRONMENT = '{{ env_suffix | replace("_", "") }}',
            {{ gov_db }}.CORE.OWNER = 'MARKETING_TEAM',
            {{ gov_db }}.CORE.DOMAIN = 'MARKETING',
            {{ gov_db }}.CORE.COST_CENTER = 'MKT_01';

ALTER DATABASE {{ eco_db }} 
    SET TAG {{ gov_db }}.CORE.ENVIRONMENT = '{{ env_suffix | replace("_", "") }}',
            {{ gov_db }}.CORE.OWNER = 'ECOMMERCE_TEAM',
            {{ gov_db }}.CORE.DOMAIN = 'ECOMMERCE',
            {{ gov_db }}.CORE.COST_CENTER = 'ECO_01';

ALTER DATABASE {{ ret_db }} 
    SET TAG {{ gov_db }}.CORE.ENVIRONMENT = '{{ env_suffix | replace("_", "") }}',
            {{ gov_db }}.CORE.OWNER = 'RETAIL_TEAM',
            {{ gov_db }}.CORE.DOMAIN = 'RETAIL',
            {{ gov_db }}.CORE.COST_CENTER = 'RET_01';

ALTER DATABASE {{ ana_db }} 
    SET TAG {{ gov_db }}.CORE.ENVIRONMENT = '{{ env_suffix | replace("_", "") }}',
            {{ gov_db }}.CORE.OWNER = 'ANALYTICS_TEAM',
            {{ gov_db }}.CORE.DOMAIN = 'ANALYTICS',
            {{ gov_db }}.CORE.COST_CENTER = 'ANA_01';


-- TAG ASSIGNMENT ON WAREHOUSES

ALTER WAREHOUSE TRANSFORM_WH 
    SET TAG {{ gov_db }}.CORE.ENVIRONMENT = '{{ env_suffix | replace("_", "") }}',
            {{ gov_db }}.CORE.OWNER = 'DATA_TEAM',
            {{ gov_db }}.CORE.DOMAIN = 'ANALYTICS',
            {{ gov_db }}.CORE.COST_CENTER = 'ANA_01';

ALTER WAREHOUSE FIN_WH 
    SET TAG {{ gov_db }}.CORE.ENVIRONMENT = '{{ env_suffix | replace("_", "") }}',
            {{ gov_db }}.CORE.OWNER = 'FINANCE_TEAM',
            {{ gov_db }}.CORE.DOMAIN = 'FINANCE',
            {{ gov_db }}.CORE.COST_CENTER = 'FIN_01';

ALTER WAREHOUSE MKT_WH 
    SET TAG {{ gov_db }}.CORE.ENVIRONMENT = '{{ env_suffix | replace("_", "") }}',
            {{ gov_db }}.CORE.OWNER = 'MARKETING_TEAM',
            {{ gov_db }}.CORE.DOMAIN = 'MARKETING',
            {{ gov_db }}.CORE.COST_CENTER = 'MKT_01';

ALTER WAREHOUSE ECO_WH 
    SET TAG {{ gov_db }}.CORE.ENVIRONMENT = '{{ env_suffix | replace("_", "") }}',
            {{ gov_db }}.CORE.OWNER = 'ECOMMERCE_TEAM',
            {{ gov_db }}.CORE.DOMAIN = 'ECOMMERCE',
            {{ gov_db }}.CORE.COST_CENTER = 'ECO_01';

ALTER WAREHOUSE RET_WH 
    SET TAG {{ gov_db }}.CORE.ENVIRONMENT = '{{ env_suffix | replace("_", "") }}',
            {{ gov_db }}.CORE.OWNER = 'RETAIL_TEAM',
            {{ gov_db }}.CORE.DOMAIN = 'RETAIL',
            {{ gov_db }}.CORE.COST_CENTER = 'RET_01';

ALTER WAREHOUSE ANA_WH 
    SET TAG {{ gov_db }}.CORE.ENVIRONMENT = '{{ env_suffix | replace("_", "") }}',
            {{ gov_db }}.CORE.OWNER = 'ANALYTICS_TEAM',
            {{ gov_db }}.CORE.DOMAIN = 'ANALYTICS',
            {{ gov_db }}.CORE.COST_CENTER = 'ANA_01';