### DCM Delivery procedure

**Step 0 — Prerequisites (first DCM creation) bootstrap only**
Owner: ACCOUNTADMIN (Script to be executed by ACCOUNTADMIN)
Execute the script /scripts/bootstrap_pre_deploy_DCM.sql
This script will :
    * Create the parent database
    * Create the DCM container schema
    * Create the deployer role
    * Grant the deployer role to xxx Users
    * Grant initial privileges to DCM_DEPLOYER
    * Create the DCM project + Grant ownership to DCM_DEPLOYER


From now on, DCM_DEPLOYER role should do the delivery
**Step 1 — Analyze (definition validation)**
```bash
snow dcm raw-analyze MGMT_DB_DEV.DCM.COST_GOVERNANCE_DCM_DEV \
  --project-dir /workspace/dcm_cost_governance \
  --target DEV \
  --role DCM_DEPLOYER \
  --save-output
```

**Step 2 — Plan (dry run)**
```bash
snow dcm plan MGMT_DB_DEV.DCM.COST_GOVERNANCE_DCM_DEV \
  --project-dir /workspace/dcm_cost_governance \
  --target DEV \
  --role DCM_DEPLOYER \
  --save-output
```

**Step 2.1 — Review plan output**
Inspect out/plan/plan_result.json
Verify: no unexpected DROP or ALTER operations

**Step 3 — Deploy**
```bash
snow dcm deploy MGMT_DB_DEV.DCM.COST_GOVERNANCE_DCM_DEV \
  --project-dir /workspace/dcm_cost_governance \
  --target DEV \
  --role DCM_DEPLOYER \
  --alias v1.0-dev
```