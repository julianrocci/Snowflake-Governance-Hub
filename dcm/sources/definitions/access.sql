{% macro _domain_roles(db) %}
DEFINE DATABASE ROLE {{db}}.READER
    COMMENT = 'Read-only access to MARTS';

DEFINE DATABASE ROLE {{db}}.ANALYST
    COMMENT = 'Read access to STAGING + MARTS';

DEFINE DATABASE ROLE {{db}}.ADMIN
    COMMENT = 'Full access — DDL + DML on all schemas';

GRANT DATABASE ROLE {{db}}.READER TO DATABASE ROLE {{db}}.ANALYST;
GRANT DATABASE ROLE {{db}}.ANALYST TO DATABASE ROLE {{db}}.ADMIN;
GRANT DATABASE ROLE {{db}}.ADMIN TO ROLE SYSADMIN;

GRANT USAGE ON DATABASE {{db}} TO DATABASE ROLE {{db}}.READER;
GRANT USAGE ON SCHEMA {{db}}.MARTS TO DATABASE ROLE {{db}}.READER;
GRANT SELECT ON ALL TABLES IN SCHEMA {{db}}.MARTS TO DATABASE ROLE {{db}}.READER;

GRANT USAGE ON SCHEMA {{db}}.STAGING TO DATABASE ROLE {{db}}.ANALYST;
GRANT SELECT ON ALL TABLES IN SCHEMA {{db}}.STAGING TO DATABASE ROLE {{db}}.ANALYST;

GRANT USAGE ON SCHEMA {{db}}.RAW TO DATABASE ROLE {{db}}.ADMIN;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA {{db}}.RAW TO DATABASE ROLE {{db}}.ADMIN;
GRANT CREATE TABLE, CREATE VIEW ON SCHEMA {{db}}.RAW TO DATABASE ROLE {{db}}.ADMIN;
GRANT CREATE TABLE, CREATE VIEW ON SCHEMA {{db}}.STAGING TO DATABASE ROLE {{db}}.ADMIN;
GRANT CREATE TABLE, CREATE VIEW ON SCHEMA {{db}}.MARTS TO DATABASE ROLE {{db}}.ADMIN;

{% endmacro %}

{% macro _domain_account_roles(db, wh_prefix) %}
DEFINE ROLE {{wh_prefix}}_READER{{env_suffix}}
    COMMENT = 'Account role — read MARTS + warehouse access for {{db}}';
GRANT DATABASE ROLE {{db}}.READER TO ROLE {{wh_prefix}}_READER{{env_suffix}};
GRANT ROLE {{wh_prefix}}_WH{{env_suffix}}_USER TO ROLE {{wh_prefix}}_READER{{env_suffix}};
GRANT ROLE {{wh_prefix}}_READER{{env_suffix}} TO ROLE SYSADMIN;

DEFINE ROLE {{wh_prefix}}_ANALYST{{env_suffix}}
    COMMENT = 'Account role — read STAGING + MARTS + warehouse access for {{db}}';
GRANT DATABASE ROLE {{db}}.ANALYST TO ROLE {{wh_prefix}}_ANALYST{{env_suffix}};
GRANT ROLE {{wh_prefix}}_WH{{env_suffix}}_USER TO ROLE {{wh_prefix}}_ANALYST{{env_suffix}};
GRANT ROLE {{wh_prefix}}_READER{{env_suffix}} TO ROLE {{wh_prefix}}_ANALYST{{env_suffix}};
GRANT ROLE {{wh_prefix}}_ANALYST{{env_suffix}} TO ROLE SYSADMIN;

DEFINE ROLE {{wh_prefix}}_ADMIN{{env_suffix}}
    COMMENT = 'Account role — full access + warehouse for {{db}}';
GRANT DATABASE ROLE {{db}}.ADMIN TO ROLE {{wh_prefix}}_ADMIN{{env_suffix}};
GRANT ROLE {{wh_prefix}}_WH{{env_suffix}}_USER TO ROLE {{wh_prefix}}_ADMIN{{env_suffix}};
GRANT ROLE {{wh_prefix}}_ANALYST{{env_suffix}} TO ROLE {{wh_prefix}}_ADMIN{{env_suffix}};
GRANT ROLE {{wh_prefix}}_ADMIN{{env_suffix}} TO ROLE SYSADMIN;
{% endmacro %}

{% macro _analytics_roles(db) %}
DEFINE DATABASE ROLE {{db}}.READER
    COMMENT = 'Read-only access to cross-domain MARTS';

DEFINE DATABASE ROLE {{db}}.ADMIN
    COMMENT = 'Full access — DDL + DML on MARTS';

GRANT DATABASE ROLE {{db}}.READER TO DATABASE ROLE {{db}}.ADMIN;
GRANT DATABASE ROLE {{db}}.ADMIN TO ROLE SYSADMIN;

GRANT USAGE ON DATABASE {{db}} TO DATABASE ROLE {{db}}.READER;
GRANT USAGE ON SCHEMA {{db}}.MARTS TO DATABASE ROLE {{db}}.READER;
GRANT SELECT ON ALL TABLES IN SCHEMA {{db}}.MARTS TO DATABASE ROLE {{db}}.READER;

GRANT CREATE TABLE, CREATE VIEW, CREATE DYNAMIC TABLE ON SCHEMA {{db}}.MARTS TO DATABASE ROLE {{db}}.ADMIN;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA {{db}}.MARTS TO DATABASE ROLE {{db}}.ADMIN;
{% endmacro %}


-- WAREHOUSE ACCESS (account roles — WH grants need account roles)

{% for wh in warehouses %}
DEFINE ROLE {{wh.name}}_WH{{env_suffix}}_USER
    COMMENT = 'Warehouse usage — {{wh.comment}}';

GRANT USAGE ON WAREHOUSE {{wh.name}}_WH{{env_suffix}} TO ROLE {{wh.name}}_WH{{env_suffix}}_USER;
GRANT ROLE {{wh.name}}_WH{{env_suffix}}_USER TO ROLE SYSADMIN;
{% endfor %}


-- DOMAIN DATABASE ROLES (3-tier: READER → ANALYST → ADMIN)

{{ _domain_roles(fin_db) }}
{{ _domain_roles(mkt_db) }}
{{ _domain_roles(eco_db) }}
{{ _domain_roles(ret_db) }}
{{ _domain_roles(loy_db) }}
{{ _domain_roles(sal_db) }}
{{ _domain_roles(hr_db) }}

{{ _analytics_roles(ana_db) }}


-- DOMAIN ACCOUNT ROLES (bundle: database role + warehouse — assignable to users)

{{ _domain_account_roles(fin_db, 'FIN') }}
{{ _domain_account_roles(mkt_db, 'MKT') }}
{{ _domain_account_roles(eco_db, 'ECO') }}
{{ _domain_account_roles(ret_db, 'RET') }}
{{ _domain_account_roles(loy_db, 'LOY') }}
{{ _domain_account_roles(sal_db, 'SAL') }}
{{ _domain_account_roles(hr_db, 'HR') }}


-- MANAGEMENT ACCOUNT ROLES (bundle: governance db roles + warehouse)

DEFINE ROLE MGMT_READER{{env_suffix}}
    COMMENT = 'Account role — read governance schemas + warehouse access';
GRANT DATABASE ROLE {{mgmt_db}}.GOVERNANCE_READER TO ROLE MGMT_READER{{env_suffix}};
GRANT ROLE MGMT_WH{{env_suffix}}_USER TO ROLE MGMT_READER{{env_suffix}};
GRANT ROLE MGMT_READER{{env_suffix}} TO ROLE SYSADMIN;

DEFINE ROLE MGMT_ADMIN{{env_suffix}}
    COMMENT = 'Account role — full governance access + warehouse';
GRANT DATABASE ROLE {{mgmt_db}}.GOVERNANCE_ADMIN TO ROLE MGMT_ADMIN{{env_suffix}};
GRANT ROLE MGMT_WH{{env_suffix}}_USER TO ROLE MGMT_ADMIN{{env_suffix}};
GRANT ROLE MGMT_READER{{env_suffix}} TO ROLE MGMT_ADMIN{{env_suffix}};
GRANT ROLE MGMT_ADMIN{{env_suffix}} TO ROLE SYSADMIN;


-- MANAGEMENT DB ROLES

DEFINE DATABASE ROLE {{mgmt_db}}.GOVERNANCE_READER
    COMMENT = 'Read-only access to governance schemas';

DEFINE DATABASE ROLE {{mgmt_db}}.GOVERNANCE_ADMIN
    COMMENT = 'Full access to governance and config schemas';

GRANT DATABASE ROLE {{mgmt_db}}.GOVERNANCE_READER TO DATABASE ROLE {{mgmt_db}}.GOVERNANCE_ADMIN;
GRANT DATABASE ROLE {{mgmt_db}}.GOVERNANCE_ADMIN TO ROLE SYSADMIN;

GRANT USAGE ON DATABASE {{mgmt_db}} TO DATABASE ROLE {{mgmt_db}}.GOVERNANCE_READER;
GRANT USAGE ON SCHEMA {{mgmt_db}}.COST_GOVERNANCE TO DATABASE ROLE {{mgmt_db}}.GOVERNANCE_READER;
GRANT SELECT ON ALL TABLES IN SCHEMA {{mgmt_db}}.COST_GOVERNANCE TO DATABASE ROLE {{mgmt_db}}.GOVERNANCE_READER;

GRANT USAGE ON SCHEMA {{mgmt_db}}.ACCESS_GOVERNANCE TO DATABASE ROLE {{mgmt_db}}.GOVERNANCE_READER;
GRANT SELECT ON ALL TABLES IN SCHEMA {{mgmt_db}}.ACCESS_GOVERNANCE TO DATABASE ROLE {{mgmt_db}}.GOVERNANCE_READER;


GRANT CREATE TABLE, CREATE VIEW ON SCHEMA {{mgmt_db}}.COST_GOVERNANCE TO DATABASE ROLE {{mgmt_db}}.GOVERNANCE_ADMIN;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA {{mgmt_db}}.COST_GOVERNANCE TO DATABASE ROLE {{mgmt_db}}.GOVERNANCE_ADMIN;

GRANT CREATE TABLE, CREATE VIEW ON SCHEMA {{mgmt_db}}.ACCESS_GOVERNANCE TO DATABASE ROLE {{mgmt_db}}.GOVERNANCE_ADMIN;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA {{mgmt_db}}.ACCESS_GOVERNANCE TO DATABASE ROLE {{mgmt_db}}.GOVERNANCE_ADMIN;

GRANT USAGE ON SCHEMA {{mgmt_db}}.CONFIG TO DATABASE ROLE {{mgmt_db}}.GOVERNANCE_ADMIN;
GRANT CREATE TABLE, CREATE VIEW ON SCHEMA {{mgmt_db}}.CONFIG TO DATABASE ROLE {{mgmt_db}}.GOVERNANCE_ADMIN;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA {{mgmt_db}}.CONFIG TO DATABASE ROLE {{mgmt_db}}.GOVERNANCE_ADMIN;

GRANT USAGE ON SCHEMA {{mgmt_db}}.ORCHESTRATION TO DATABASE ROLE {{mgmt_db}}.GOVERNANCE_ADMIN;
GRANT CREATE TABLE, CREATE VIEW, CREATE TASK ON SCHEMA {{mgmt_db}}.ORCHESTRATION TO DATABASE ROLE {{mgmt_db}}.GOVERNANCE_ADMIN;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA {{mgmt_db}}.ORCHESTRATION TO DATABASE ROLE {{mgmt_db}}.GOVERNANCE_ADMIN;


-- INGESTION ROLE (data engineering — stages, pipes, loading into RAW)

DEFINE ROLE INGESTION_ROLE{{env_suffix}}
    COMMENT = 'Service role for data ingestion — creates stages, pipes and loads into RAW schemas';

GRANT ROLE INGESTION_ROLE{{env_suffix}} TO ROLE SYSADMIN;
GRANT USAGE ON WAREHOUSE TRANSFORM_WH{{env_suffix}} TO ROLE INGESTION_ROLE{{env_suffix}};

{% for db_var in [fin_db, mkt_db, eco_db, ret_db, loy_db] %}
GRANT USAGE ON DATABASE {{db_var}} TO ROLE INGESTION_ROLE{{env_suffix}};
GRANT USAGE ON SCHEMA {{db_var}}.RAW TO ROLE INGESTION_ROLE{{env_suffix}};
GRANT CREATE TABLE, CREATE STAGE, CREATE PIPE, CREATE FILE FORMAT ON SCHEMA {{db_var}}.RAW TO ROLE INGESTION_ROLE{{env_suffix}};
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA {{db_var}}.RAW TO ROLE INGESTION_ROLE{{env_suffix}};
{% endfor %}


-- TRANSFORM ROLE (dbt / ELT service account)

DEFINE ROLE TRANSFORM_ROLE{{env_suffix}}
    COMMENT = 'Service role for dbt/ELT — reads RAW, writes STAGING + MARTS across all domains';

GRANT ROLE TRANSFORM_ROLE{{env_suffix}} TO ROLE SYSADMIN;
GRANT USAGE ON WAREHOUSE TRANSFORM_WH{{env_suffix}} TO ROLE TRANSFORM_ROLE{{env_suffix}};

{% for db_var in [fin_db, mkt_db, eco_db, ret_db, loy_db] %}
GRANT USAGE ON DATABASE {{db_var}} TO ROLE TRANSFORM_ROLE{{env_suffix}};
GRANT USAGE ON SCHEMA {{db_var}}.RAW TO ROLE TRANSFORM_ROLE{{env_suffix}};
GRANT SELECT ON ALL TABLES IN SCHEMA {{db_var}}.RAW TO ROLE TRANSFORM_ROLE{{env_suffix}};

GRANT USAGE ON SCHEMA {{db_var}}.STAGING TO ROLE TRANSFORM_ROLE{{env_suffix}};
GRANT CREATE TABLE, CREATE VIEW ON SCHEMA {{db_var}}.STAGING TO ROLE TRANSFORM_ROLE{{env_suffix}};
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA {{db_var}}.STAGING TO ROLE TRANSFORM_ROLE{{env_suffix}};

GRANT USAGE ON SCHEMA {{db_var}}.MARTS TO ROLE TRANSFORM_ROLE{{env_suffix}};
GRANT CREATE TABLE, CREATE VIEW, CREATE DYNAMIC TABLE ON SCHEMA {{db_var}}.MARTS TO ROLE TRANSFORM_ROLE{{env_suffix}};
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA {{db_var}}.MARTS TO ROLE TRANSFORM_ROLE{{env_suffix}};
{% endfor %}

GRANT USAGE ON DATABASE {{ana_db}} TO ROLE TRANSFORM_ROLE{{env_suffix}};
GRANT USAGE ON SCHEMA {{ana_db}}.MARTS TO ROLE TRANSFORM_ROLE{{env_suffix}};
GRANT CREATE TABLE, CREATE VIEW, CREATE DYNAMIC TABLE ON SCHEMA {{ana_db}}.MARTS TO ROLE TRANSFORM_ROLE{{env_suffix}};
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA {{ana_db}}.MARTS TO ROLE TRANSFORM_ROLE{{env_suffix}};

GRANT USAGE ON DATABASE {{mgmt_db}} TO ROLE TRANSFORM_ROLE{{env_suffix}};
GRANT USAGE ON SCHEMA {{mgmt_db}}.COST_GOVERNANCE TO ROLE TRANSFORM_ROLE{{env_suffix}};
GRANT CREATE TABLE, CREATE VIEW, CREATE DYNAMIC TABLE ON SCHEMA {{mgmt_db}}.COST_GOVERNANCE TO ROLE TRANSFORM_ROLE{{env_suffix}};
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA {{mgmt_db}}.COST_GOVERNANCE TO ROLE TRANSFORM_ROLE{{env_suffix}};
GRANT USAGE ON SCHEMA {{mgmt_db}}.ACCESS_GOVERNANCE TO ROLE TRANSFORM_ROLE{{env_suffix}};
GRANT CREATE TABLE, CREATE VIEW, CREATE DYNAMIC TABLE ON SCHEMA {{mgmt_db}}.ACCESS_GOVERNANCE TO ROLE TRANSFORM_ROLE{{env_suffix}};
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA {{mgmt_db}}.ACCESS_GOVERNANCE TO ROLE TRANSFORM_ROLE{{env_suffix}};


-- CROSS-DOMAIN READ ACCESS
-- Database roles cannot be granted cross-database.
-- Use an account role as bridge: each domain READER → account role → SYSADMIN.

DEFINE ROLE ANALYTICS_CROSS_READER{{env_suffix}}
    COMMENT = 'Bridge role — aggregates READER access from all domains for Analytics';

{% for db_var in [fin_db, mkt_db, eco_db, ret_db, loy_db] %}
GRANT DATABASE ROLE {{db_var}}.READER TO ROLE ANALYTICS_CROSS_READER{{env_suffix}};
{% endfor %}

GRANT DATABASE ROLE {{ana_db}}.ADMIN TO ROLE ANALYTICS_CROSS_READER{{env_suffix}};
GRANT ROLE ANA_WH{{env_suffix}}_USER TO ROLE ANALYTICS_CROSS_READER{{env_suffix}};
GRANT ROLE ANALYTICS_CROSS_READER{{env_suffix}} TO ROLE SYSADMIN;