Project Scope — Snowflake Data Cost Governance

1. Context & Motivation

Snowflake provides a powerful and flexible data platform, but its consumption-based pricing model can quickly lead to uncontrolled costs if usage is not properly monitored and governed.

In many organizations, Snowflake costs are observed only at a high level (monthly invoice), with limited visibility into which teams, workloads, or users are responsible for the consumption.

This project aims to address this gap by designing a data cost governance layer on top of Snowflake system metadata, enabling clear cost attribution, waste detection, and actionable optimization insights.

2. Target Users

This project is designed for the following profiles:

Data Platform Owner / Head of Data
-Needs visibility into global Snowflake usage and cost drivers
-Wants to identify cost inefficiencies and justify budget decisions

Data Engineers / Analytics Engineers
-Want to understand why certain queries or warehouses are expensive
-Need concrete insights to optimize performance and resource usage

Finance / FinOps Teams
-Require reliable cost attribution by team, environment, or cost center
-Need consistent metrics for internal chargeback and forecasting

3. Problems Addressed

The project focuses on the following recurring enterprise problems:

Lack of granular visibility into Snowflake costs
Inability to attribute costs to teams, environments, or workloads
Difficulty identifying inefficient or unused resources

4. Project Objectives

The core objectives of this project are:

Provide cost visibility at multiple levels (warehouse, team, workload)
Enable cost attribution using Snowflake tags
Identify inefficient usage patterns and potential waste
Produce actionable recommendations for cost optimization
Establish a repeatable and auditable cost governance framework

5. Key Decisions Enabled

The cost governance layer is designed to support concrete operational and strategic decisions, such as:

Resizing or consolidating Snowflake warehouses
Enforcing workload isolation strategies
Removing unused or underutilized resources
Defining and enforcing tagging standards
Supporting internal chargeback or showback models

6. Key Metrics (KPIs)

The project exposes the following core metrics:

Total Snowflake cost per day and per month
Cost by warehouse
Cost by team and environment (via tags)
Cost by workload type (BI, ETL, ML)
Top cost-generating users and queries
Estimated percentage of inefficient usage

7. Out of Scope

To maintain focus and realism, the following topics are intentionally excluded from this project:

Automated cost optimization or self-healing systems
Machine learning–based cost prediction
Cloud provider–level billing reconciliation

8. Learning Objectives

Beyond the functional goals, this project also serves as a structured learning exercise to deepen myunderstanding of:

Snowflake cost and billing mechanisms
Warehouse sizing and workload management
Query performance analysis
Tag-based cost attribution
Enterprise data governance principles
