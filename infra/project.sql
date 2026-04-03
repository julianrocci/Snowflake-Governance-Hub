/* 
   Creating the technical management containers in the MGMT database for DCM Project with each ENV
*/

USE ROLE DCM_ADMIN;

-- Project for Development
CREATE DCM PROJECT IF NOT EXISTS {{ mgmt_db }}.DCM.COST_GOV_PROJECT_DEV
    COMMENT = 'Cost Governance Infrastructure - Development Management';

-- Project for Production
CREATE DCM PROJECT IF NOT EXISTS {{ mgmt_db }}.DCM.COST_GOV_PROJECT_PROD
    COMMENT = 'Cost Governance Infrastructure - Production Management';