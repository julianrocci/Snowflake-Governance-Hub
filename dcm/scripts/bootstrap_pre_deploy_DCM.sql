-- =============================================================================
-- DCM Cost Governance — Pre-deploy Setup Procedure
-- Run as ACCOUNTADMIN. Creates parent DB, schema, deployer role, grants, and DCM project.
-- =============================================================================

CREATE OR REPLACE PROCEDURE SETUP_DCM_COST_GOVERNANCE(ENV VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
DECLARE
    mgmt_db VARCHAR;
    env_suffix VARCHAR;
    dcm_project VARCHAR;
BEGIN
    CASE UPPER(ENV)
        WHEN 'DEV'  THEN mgmt_db := 'MGMT_DB_DEV';  env_suffix := '_DEV';
        WHEN 'UAT'  THEN mgmt_db := 'MGMT_DB_UAT';  env_suffix := '_UAT';
        WHEN 'PROD' THEN mgmt_db := 'MGMT_DB';      env_suffix := '';
        ELSE RETURN 'ERROR: Invalid environment. Use DEV, UAT, or PROD.';
    END CASE;

    dcm_project := mgmt_db || '.DCM.COST_GOVERNANCE_DCM' || env_suffix;

    -- Step 1: Parent database
    EXECUTE IMMEDIATE 'CREATE DATABASE IF NOT EXISTS ' || mgmt_db;

    -- Step 2: DCM container schema
    EXECUTE IMMEDIATE 'CREATE SCHEMA IF NOT EXISTS ' || mgmt_db || '.DCM';

    -- Step 3: Deployer role
    EXECUTE IMMEDIATE 'CREATE ROLE IF NOT EXISTS DCM_DEPLOYER';
    EXECUTE IMMEDIATE 'GRANT ROLE DCM_DEPLOYER TO ROLE SYSADMIN';

    -- Step 4a: Grants — parent database
    EXECUTE IMMEDIATE 'GRANT ALL PRIVILEGES ON DATABASE ' || mgmt_db || ' TO ROLE DCM_DEPLOYER';

    -- Step 4b: Grants — DCM container schema (needed for stage uploads during deploy)
    EXECUTE IMMEDIATE 'GRANT ALL ON SCHEMA ' || mgmt_db || '.DCM TO ROLE DCM_DEPLOYER';

    -- Step 5: Grants — account-level
    EXECUTE IMMEDIATE 'GRANT CREATE ROLE ON ACCOUNT TO ROLE DCM_DEPLOYER';
    EXECUTE IMMEDIATE 'GRANT CREATE DATABASE ON ACCOUNT TO ROLE DCM_DEPLOYER';
    EXECUTE IMMEDIATE 'GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE DCM_DEPLOYER';
    EXECUTE IMMEDIATE 'GRANT MANAGE GRANTS ON ACCOUNT TO ROLE DCM_DEPLOYER';

    -- Step 6: DCM project object
    EXECUTE IMMEDIATE 'CREATE DCM PROJECT IF NOT EXISTS ' || dcm_project ||
        ' COMMENT = ''Cost governance DCM project - ' || UPPER(ENV) || ' environment''';

    -- Step 7: Transfer ownership (skip if DCM_DEPLOYER already owns it)
    EXECUTE IMMEDIATE 'SHOW DCM PROJECTS LIKE ''COST_GOVERNANCE_DCM' || env_suffix ||
        ''' IN SCHEMA ' || mgmt_db || '.DCM';
    LET owner_check VARCHAR := (SELECT "owner" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) LIMIT 1);
    IF (owner_check IS NULL OR owner_check != 'DCM_DEPLOYER') THEN
        EXECUTE IMMEDIATE 'GRANT OWNERSHIP ON DCM PROJECT ' || dcm_project ||
            ' TO ROLE DCM_DEPLOYER COPY CURRENT GRANTS';
    END IF;

    -- Step 8: CI/CD service user (idempotent — created once, receives both roles)
    EXECUTE IMMEDIATE 'CREATE USER IF NOT EXISTS SVC_CICD_DEPLOY DEFAULT_ROLE = DCM_DEPLOYER';
    EXECUTE IMMEDIATE 'GRANT ROLE DCM_DEPLOYER TO USER SVC_CICD_DEPLOY';
    EXECUTE IMMEDIATE 'GRANT ROLE SYSADMIN TO USER SVC_CICD_DEPLOY';
    EXECUTE IMMEDIATE 'GRANT ROLE ACCOUNTADMIN TO USER SVC_CICD_DEPLOY';

    RETURN 'SUCCESS: DCM setup complete for ' || UPPER(ENV) || ' — project: ' || dcm_project;
END;