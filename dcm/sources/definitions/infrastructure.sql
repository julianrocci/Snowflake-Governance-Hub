DEFINE SCHEMA {{mgmt_db}}.COST_GOVERNANCE
    WITH MANAGED ACCESS
    COMMENT = 'Cost and performance governance — marts and staging'
    DATA_RETENTION_TIME_IN_DAYS = {{retention_days}};

DEFINE SCHEMA {{mgmt_db}}.ACCESS_GOVERNANCE
    WITH MANAGED ACCESS
    COMMENT = 'Audit, grants and access control monitoring'
    DATA_RETENTION_TIME_IN_DAYS = {{retention_days}};

DEFINE SCHEMA {{mgmt_db}}.ORCHESTRATION
    COMMENT = 'Tasks, scheduling and pipeline orchestration';

DEFINE SCHEMA {{mgmt_db}}.CONFIG
    COMMENT = 'Shared parameters, lookups and reference data';

DEFINE DATABASE {{fin_db}}
    COMMENT = 'Finance domain data';

DEFINE SCHEMA {{fin_db}}.RAW
    COMMENT = 'Raw ingested finance data';

DEFINE SCHEMA {{fin_db}}.STAGING
    WITH MANAGED ACCESS
    COMMENT = 'Cleaned and standardized finance data';

DEFINE SCHEMA {{fin_db}}.MARTS
    WITH MANAGED ACCESS
    COMMENT = 'Finance business models for consumption';

DEFINE DATABASE {{mkt_db}}
    COMMENT = 'Marketing domain data';

DEFINE SCHEMA {{mkt_db}}.RAW
    COMMENT = 'Raw ingested marketing data';

DEFINE SCHEMA {{mkt_db}}.STAGING
    WITH MANAGED ACCESS
    COMMENT = 'Cleaned and standardized marketing data';

DEFINE SCHEMA {{mkt_db}}.MARTS
    WITH MANAGED ACCESS
    COMMENT = 'Marketing business models for consumption';

DEFINE DATABASE {{eco_db}}
    COMMENT = 'E-Commerce domain data';

DEFINE SCHEMA {{eco_db}}.RAW
    COMMENT = 'Raw ingested e-commerce data';

DEFINE SCHEMA {{eco_db}}.STAGING
    WITH MANAGED ACCESS
    COMMENT = 'Cleaned and standardized e-commerce data';

DEFINE SCHEMA {{eco_db}}.MARTS
    WITH MANAGED ACCESS
    COMMENT = 'E-Commerce business models for consumption';

DEFINE DATABASE {{ret_db}}
    COMMENT = 'Retail domain data';

DEFINE SCHEMA {{ret_db}}.RAW
    COMMENT = 'Raw ingested retail data';

DEFINE SCHEMA {{ret_db}}.STAGING
    WITH MANAGED ACCESS
    COMMENT = 'Cleaned and standardized retail data';

DEFINE SCHEMA {{ret_db}}.MARTS
    WITH MANAGED ACCESS
    COMMENT = 'Retail business models for consumption';

DEFINE DATABASE {{ana_db}}
    COMMENT = 'Analytics cross-domain data';

DEFINE SCHEMA {{ana_db}}.MARTS
    WITH MANAGED ACCESS
    COMMENT = 'Cross-domain analytical models for consumption';

DEFINE DATABASE {{loy_db}}
    COMMENT = 'Loyalty domain data';

DEFINE SCHEMA {{loy_db}}.RAW
    COMMENT = 'Raw ingested loyalty data';

DEFINE SCHEMA {{loy_db}}.STAGING
    WITH MANAGED ACCESS
    COMMENT = 'Cleaned and standardized loyalty data';

DEFINE SCHEMA {{loy_db}}.MARTS
    WITH MANAGED ACCESS
    COMMENT = 'Loyalty business models for consumption';

DEFINE DATABASE {{sal_db}}
    COMMENT = 'Sales domain data';

DEFINE SCHEMA {{sal_db}}.RAW
    COMMENT = 'Raw ingested sales data';

DEFINE SCHEMA {{sal_db}}.STAGING
    WITH MANAGED ACCESS
    COMMENT = 'Cleaned and standardized sales data';

DEFINE SCHEMA {{sal_db}}.MARTS
    WITH MANAGED ACCESS
    COMMENT = 'Sales business models for consumption';

DEFINE DATABASE {{hr_db}}
    COMMENT = 'HR domain data';

DEFINE SCHEMA {{hr_db}}.RAW
    COMMENT = 'Raw ingested HR data';

DEFINE SCHEMA {{hr_db}}.STAGING
    WITH MANAGED ACCESS
    COMMENT = 'Cleaned and standardized HR data';

DEFINE SCHEMA {{hr_db}}.MARTS
    WITH MANAGED ACCESS
    COMMENT = 'HR business models for consumption';

{% for wh in warehouses %}
DEFINE WAREHOUSE {{wh.name}}_WH{{env_suffix}}
WITH
    WAREHOUSE_SIZE = '{{wh.wh_size}}'
    AUTO_SUSPEND = {{wh.auto_suspend}}
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = '{{wh.comment}}';
{% endfor %}