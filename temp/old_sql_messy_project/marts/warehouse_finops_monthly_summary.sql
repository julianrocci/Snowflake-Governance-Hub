-- Monthly FinOps Summary with Financial Impact

WITH base AS (

    SELECT
        warehouse_name,
        DATE_TRUNC('month', usage_date) AS usage_month,

        estimated_waste_seconds,
        estimated_waste_ratio,
        credits_per_hour  -- assume this comes from your optimization model

    FROM {{ ref('warehouse_optimization_model') }}

)

SELECT
    warehouse_name,
    usage_month,

    SUM(estimated_waste_seconds) AS total_monthly_waste_seconds,

    AVG(estimated_waste_ratio) AS avg_monthly_waste_ratio,

    -- Financial impact
    ROUND(SUM(estimated_waste_seconds) / 3600 * MAX(credits_per_hour), 2) AS estimated_monthly_waste_credits,

    ROUND(SUM(estimated_waste_seconds) / 3600 * MAX(credits_per_hour) * 3, 2) AS estimated_monthly_waste_usd,

    RANK() OVER (
        PARTITION BY usage_month
        ORDER BY SUM(estimated_waste_seconds) DESC
    ) AS monthly_waste_rank

FROM base

GROUP BY warehouse_name, usage_month
ORDER BY usage_month DESC, monthly_waste_rank;