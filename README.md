**Focus : Data Quality | Cost Optimization | Performance Tuning**

This project demonstrates a production-ready framework designed to govern, monitor, and optimize your Snowflake environment. It shows the "hidden costs" problem by providing full transparency and actionable control over data operations. It also enforces automated quality standards and ensures security.

<details>
<summary>📁 View Project Structure </summary>

```
cost_governance/                                          ← Git repo root
├── dcm/                                                  ← IaC (DCM)
│   ├── manifest.yml
│   ├── sources/definitions/
│   │   ├── infrastructure.sql                            (databases, schemas, warehouses)
│   │   ├── access.sql                                    (roles, grants)
│   │   ├── tables.sql                                    (config tables)
│   │   └── tags.sql                                      (governance tags)
│   └── scripts/
│       ├── bootstrap_pre_deploy_DCM.sql                  (one-shot setup)
│       ├── bootstrap_secondary_roles_post_deploy_DCM.sql
│       └── post_deploy_DCM_future_grants.sql             (after each DCM deploy)
├── dbt/                                                  ← Transformations (dbt)
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── macros/
│   │   └── domain_mapping.sql                            (domain/env resolution)
│   └── models/
│       ├── staging/
│       │   ├── src_snowflake.yml                         (source definitions)
│       │   ├── _stg__models.yml                          (tests)
│       │   ├── stg_warehouse_metering.sql
│       │   └── ...
│       ├── intermediate/
│       │   ├── cost/
│       │   │   ├── int_warehouse_efficiency_summary.sql
│       │   │   └── ...
│       │   └── performance/
│       │       ├── int_warehouse_spilling_performance.sql
│       │       └── ...
│       └── marts/
│           ├── cost/
│           │   ├── _cost__models.yml                      (tests)
│           │   ├── fct_warehouse_efficiency.sql
│           │   └── ...
│           └── performance/
│               ├── _performance__models.yml               (tests)
│               ├── fct_warehouse_cache_performance.sql
│               └── ...
├── .github/workflows/                                     ← CI/CD (In Progress)
├── .gitignore
├── DELIVERY_LOG.md
└── README.md
  ```

</details>

**Tech Stack**

Storage & Compute: Snowflake

Transformation: dbt (Data Build Tool)

IaC / DevOps: DCM (Database Change Management) – Ensuring automated, auditable, and version-controlled infrastructure.

Visualization: Advanced Analytics Dashboards (Cost & Performance tracking).

**Key Pillars & Solutions**

❄️ Data Quality & Observability

Data Integrity: Automated data quality checks and governance-ready metrics to ensure platform reliability

Data Freshness: Monitoring SLA/SLOs for critical pipelines to ensure data is updated within expected timeframes (Target Lag).

Trust & Transparency: Implementation of data lineage and quality dashboards to provide a "single source of truth" that users can actually trust.

❄️ Cost Optimization (FinOps)

Visibility: Attribution of costs by Team, Domain, Environment, and Workload using metadata & tags.

Waste Detection: Identification of oversized warehouses, unused resources, and "expensive" query patterns.

❄️ Data Governance & Security

Access Control: Robust RBAC (Role-Based Access Control) hierarchy.

Tagging Standards: Enforcing metadata standards.

PII Management: Secure handling of sensitive data (masking/security policies).

❄️ Performance Tuning

Query Efficiency: Monitoring and optimizing high-latency/high-cost queries (unused cache, data spilling, serverless features, etc..).

Warehouse Sizing: Strategic management of compute resources to balance speed and credit consumption.

ELT Reliability: Modern dbt-driven pipelines for clean, testable, and scalable data.

**Decision-Ready Analytics**

The project includes a dedicated analytics layer (Dashboards) to enable:

Resizing decisions based on actual compute usage.

Workload isolation strategies.

Automatic alerting on cost anomalies.
