/* Domain-based database orchestration  */

USE ROLE DCM_ADMIN;

-- MANAGEMENT LAYER

CREATE DATABASE IF NOT EXISTS {{ mgmt_db }}
    COMMENT = 'Technical database for DCM project states and metadata';

CREATE SCHEMA IF NOT EXISTS {{ mgmt_db }}.DCM
    COMMENT = 'Schema for Declarative Content Management objects';


-- CENTRAL GOVERNANCE HUB

CREATE DATABASE IF NOT EXISTS {{ gov_db }}
    DATA_RETENTION_TIME_IN_DAYS = 1
    COMMENT = 'Centralized Governance Hub - Env: {{ env_suffix }}';


-- DOMAIN SPECIFIC DATABASES

CREATE DATABASE IF NOT EXISTS {{ fin_db }} 
    COMMENT = 'Finance domain storage';

CREATE DATABASE IF NOT EXISTS {{ mkt_db }} 
    COMMENT = 'Marketing domain storage';

CREATE DATABASE IF NOT EXISTS {{ eco_db }} 
    COMMENT = 'E-commerce domain storage';

CREATE DATABASE IF NOT EXISTS {{ ret_db }} 
    COMMENT = 'Retail domain storage';

CREATE DATABASE IF NOT EXISTS {{ ana_db }} 
    COMMENT = 'Analytics domain storage';


-- CORE GOVERNANCE SCHEMAS

CREATE SCHEMA IF NOT EXISTS {{ gov_db }}.CORE
    COMMENT = 'Dedicated schema for shared governance objects (Tags, Policies)';

CREATE SCHEMA IF NOT EXISTS {{ gov_db }}.STAGING
    COMMENT = 'Cleaning layer for source data';

CREATE SCHEMA IF NOT EXISTS {{ gov_db }}.INTERMEDIATE
    COMMENT = 'Business logic and cost allocation layer';

CREATE SCHEMA IF NOT EXISTS {{ gov_db }}.MART_COST
    COMMENT = 'Final reporting: Credit and USD Consumption';

CREATE SCHEMA IF NOT EXISTS {{ gov_db }}.MART_PERFORMANCE
    COMMENT = 'Final reporting: Warehouse and Query Efficiency';

CREATE SCHEMA IF NOT EXISTS {{ gov_db }}.MART_GOVERNANCE
    COMMENT = 'Final reporting: RBAC and Security audit';