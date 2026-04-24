# User & Grants Manager

Streamlit in Snowflake application for identity and access governance across multi-domain, multi-environment Snowflake deployments.

## Overview

This app provides a centralized UI for managing Snowflake users and their domain-level access across DEV, UAT, and PROD environments. It enforces a consistent RBAC model where each business domain (Finance, Marketing, E-Commerce, etc.) has dedicated Reader and Admin roles, along with warehouse usage grants.

Restricted to ACCOUNTADMIN, SECURITYADMIN roles

## Features

### Create Users
- Create one or multiple users at once (comma-separated)
- Assign domain access (Reader or Admin) per environment
- Bulk "All Domains" mode with per-domain override
- Clone mode: replicate an existing user's grants to new users
- Warehouse usage roles automatically granted alongside domain access

### Manage Users
- View user info (login, email, creation date, last login, status)
- Change default role
- Modify domain access across environments (grant, revoke, upgrade, downgrade)
- Enable / disable users with confirmation checkbox

### Audit Trail
- Filterable log of all actions (create, update grants, disable, enable)
- Filter by action type, environment, and object type
- Paginated results (10 per page)
- Backed by `MGMT_DB.USER_MANAGEMENT.USER_ACTIVITY_LOG`

## Architecture

| Component | Location |
|-----------|----------|
| App code | `@MGMT_DB.USER_MANAGEMENT.USER_GRANTS_MANAGER_STAGE/streamlit/user_grants_manager/app.py` |
| Audit table | `MGMT_DB.USER_MANAGEMENT.USER_ACTIVITY_LOG` |
| Streamlit object | `MGMT_DB.USER_MANAGEMENT.USER_GRANTS_MANAGER` |
| Query warehouse | `COMPUTE_WH` |

## Domain & Role Model

8 business domains, each with a Reader and Admin database role per environment:

| Domain | Prefix | Reader Role (DEV) | Admin Role (DEV) | WH Usage Role (DEV) |
|--------|--------|-------------------|-------------------|----------------------|
| Finance | FIN | FIN_READER_DEV | FIN_ADMIN_DEV | FIN_WH_DEV_USER |
| Marketing | MKT | MKT_READER_DEV | MKT_ADMIN_DEV | MKT_WH_DEV_USER |
| E-Commerce | ECO | ECO_READER_DEV | ECO_ADMIN_DEV | ECO_WH_DEV_USER |
| Retail | RET | RET_READER_DEV | RET_ADMIN_DEV | RET_WH_DEV_USER |
| Loyalty | LOY | LOY_READER_DEV | LOY_ADMIN_DEV | LOY_WH_DEV_USER |
| Management | MGMT | MGMT_READER_DEV | MGMT_ADMIN_DEV | MGMT_WH_DEV_USER |
| Sales | SAL | SAL_READER_DEV | SAL_ADMIN_DEV | SAL_WH_DEV_USER |
| HR | HR | HR_READER_DEV | HR_ADMIN_DEV | HR_WH_DEV_USER |

Same pattern for UAT (`_UAT` suffix) and PROD (no suffix).

## Deployment

Deployed via the `bootstrap_pre_deploy_DCM.sql` procedure which creates the schema, stage, audit table, and Streamlit object. App code is uploaded from the workspace to the internal stage.