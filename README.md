# ❄️ Snowflake & dbt Core Hub

<p align="center">
  <img src="https://img.shields.io/badge/Snowflake-29B5E8?style=for-the-badge&logo=snowflake&logoColor=white" alt="Snowflake">
  <img src="https://img.shields.io/badge/dbt-FF694B?style=for-the-badge&logo=dbt&logoColor=white" alt="dbt">
</p>

> **Core Focus:** Analytics Engineering | Cost Optimization (FinOps) | Performance Tuning | GitOps Governance

This project demonstrates a production-ready framework designed to govern, monitor, and optimize a modern data stack. It bridges the gap between **Cloud Data Warehousing (Snowflake)** and **Modern Data Transformation Engineering (dbt)** to solve the "hidden costs" problem, enforce automated data quality, and build highly scalable analytics platforms.

---

## Key Implemented Features

### 🛠️ GitOps Infrastructure & Governance
* **DCM Infrastructure as Code:** Native automation of Snowflake environments via `Snowflake Database Change Management (DCM)` integrated into a robust **GitHub Actions CI/CD pipeline**.
* **Streamlit Access Manager App:** A secure, self-service app deployed natively inside Snowflake for multi-user creation and bulk `RBAC` grant management, backed by a dedicated audit log system.

### 💵 FinOps & Cost Optimization (`dbt Core`)
* **Cross-Entity Credit Allocation:** Automatically aggregates multi-warehouse metrics (`Ecom`, `Marketing`, `Finance`) to compute real execution-to-billing ratios and track idle capacity costs.
* **Warehouse Behavior Analyzer:** Minimizes compute waste by detecting idle gaps, excessive warehouse wakeups, and high-cost *isolated queries* (paying 60s of auto-suspend for a 2s execution).

### ⚡ Snowflake Performance Engineering (`dbt Core`)
* **Multi-Layer Cache Profiling:** Maps queries served by `Metadata/Result Cache` vs. `Local SSD` to isolate warehouse cold-starts and identify remote storage bottlenecks.
* **Query Spilling Tracker:** Automates detection of memory overflows by flagging queries with `>30%` local disk spill or `>1%` critical remote storage spilling.
* **Data Skew & Partitioning Monitor:** Catches heavy join bottlenecks by calculating execution speed per partition, highlighting critical distribution skewing (`>0.5s/partition` for `>5min`).
* **Automatic Clustering Cost Control:** Tracks monthly credit and GB consumption of Snowflake’s auto-reclustering feature to flag tables with high maintenance costs.

### 🔍 Data Observability & Quality
* **Metadata-Aware Freshness Engine:** A smart SLA tracker that accounts for Snowflake’s 3-hour data latency to correctly distinguish real data gaps from silent loading failures.

### 📊 Core Business Modeling (`dbt Core`)
* **User Cohort Retention Framework:** Analytics pipeline processing active vs. non-paying user lifecycles to extract retention insights.

---

<details>
<summary>📁 View Project Structure </summary>

```
Snowflake-Governance-Hub/                                 ← Git repo root
├── dcm/                                                  ← IaC (DCM - Database Change Management)
│   ├── manifest.yml                                      (DCM configuration)
│   ├── sources/
│   │   └── definitions/
│   │       ├── infrastructure.sql                        (databases, schemas, warehouses)
│   │       ├── access.sql                                (roles, grants, RBAC hierarchy)
│   │       ├── tables.sql                                (config & metadata tables)
│   │       └── tags.sql                                  (governance & cost allocation tags)
│   └── scripts/
│       ├── bootstrap_pre_deploy_DCM.sql                  (one-shot setup, Streamlit deployment)
│       ├── bootstrap_secondary_roles_post_deploy_DCM.sql (secondary role setup)
│       └── post_deploy_DCM_future_grants.sql             (post-deploy future grants automation)
│
├── dbt/                                                  ← Transformations (dbt - Data Build Tool)
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── macros/
│   │   └── domain_mapping.sql                            (domain & environment resolution logic)
│   ├── models/
│   │   ├── staging/
│   │   │   ├── src_snowflake.yml                         (source definitions - Account_Usage, etc)
│   │   │   ├── _stg__models.yml                          (staging model tests)
│   │   │   ├── stg_warehouse_metering.sql
│   │   │   └── ...
│   │   ├── intermediate/
│   │   │   ├── cost/
│   │   │   │   ├── int_warehouse_efficiency_summary.sql
│   │   │   │   └── ...
│   │   │   ├── performance/
│   │   │   │   ├── int_warehouse_cache_performance.sql
│   │   │   │   └── ...
│   │   │   └── quality/
│   │   │       ├── _int_quality__models.yml               (intermediate model tests)
│   │   │       ├── int_monitoring_freshness_gaps.sql
│   │   │       └── ...
│   │   └── marts/
│   │       ├── cost/
│   │       │   ├── _cost__models.yml                     (cost domain tests & specs)
│   │       │   ├── fct_warehouse_efficiency.sql
│   │       │   └── ...
│   │       ├── performance/
│   │       │   ├── _performance__models.yml              (performance domain tests & specs)
│   │       │   ├── fct_warehouse_cache_performance.sql
│   │       │   └── ...
│   │       └── quality/
│   │           ├── _quality__models.yml                  (quality domain tests & specs)
│   │           ├── fct_snowflake_monitoring_freshness.sql
│   │           └── ...
│   └── tests/
│       └── assert_no_critical_gaps.sql                   (custom test on freshness & gaps)
│
├── streamlit/                                             ← UI / Self-Service Apps
│   └── user_grants_manager/
│       ├── README.md
│       └── app.py                                        (Streamlit in Snowflake app - User & Grants manager)
│
├── docs/                                                 ← Documentation & Reference
│   ├── project_scope.md                                  (project objectives & pillars)
│   ├── dbt_concepts_memo/
│   │   └── primary_dbt_commands.md
│   └── snowflake_concepts_memo/                          (Personals memo)
│       ├── Account_Usage.md
│       ├── Caching.md
│       └── ...
│
├── .github/workflows/                                     ← CI/CD (GitHub Actions)
│   ├── deploy-dev.yml
│   └── ...
│
├── .gitignore
├── doc_delivery.md                                        (delivery process : DCM, dbt, bootstrap scripts..)
└── README.md
  ```
</details>

---

## 📈 Decision-Ready Analytics

The analytical layer generated by this framework provides enterprise-level insights to drive:
* 📉 **Resizing Decisions:** Scale down or auto-suspend compute based on empirical historical workloads.
* 💵 **Waste Elimination:** Identify and eliminate idle compute gaps, excessive warehouse wakeups, and high-cost isolated queries.
* ⚙️ **Proactive Tuning:** Detect memory overflows (Data Spilling) and join bottlenecks before they impact downstream business users.
* 🚨 **Anomaly Alerting:** Early-warning capture of query cost spikes and architectural degradation.
