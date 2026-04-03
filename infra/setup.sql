/*
   This script must be executed by an ACCOUNTADMIN to initialize the DCM_ADMIN role.
*/

USE ROLE ACCOUNTADMIN;

-- Create the dedicated DCM role
CREATE ROLE IF NOT EXISTS DCM_ADMIN
    COMMENT = 'Dedicated role for DCM project management and deployment';

-- Account-level privileges to allow Infrastructure-as-Code
GRANT CREATE DATABASE ON ACCOUNT 
    TO ROLE DCM_ADMIN;

GRANT CREATE WAREHOUSE ON ACCOUNT 
    TO ROLE DCM_ADMIN;

GRANT CREATE ROLE ON ACCOUNT 
    TO ROLE DCM_ADMIN;

-- Tag management privilege (Required for the tagging later)
GRANT APPLY TAG ON ACCOUNT 
    TO ROLE DCM_ADMIN;

-- Initial compute to allow the role to run scripts
GRANT USAGE ON WAREHOUSE TRANSFORM_WH 
    TO ROLE DCM_ADMIN;

-- Hierarchy and Assignment ( JR for my own name)
GRANT ROLE DCM_ADMIN 
    TO USER JR;

GRANT ROLE DCM_ADMIN 
    TO ROLE SYSADMIN;