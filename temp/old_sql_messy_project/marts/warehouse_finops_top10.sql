-- Top 10 most costly warehouses per month
-- With dynamic alert threshold

WITH monthly_summary AS (
    SELECT *
    FROM {{ ref('warehouse_finops_monthly_summary') }}
),

ranked_distribution AS (

    SELECT
        *,
        
        ROW_NUMBER() OVER (
            PARTITION BY usage_month
            ORDER BY estimated_monthly_waste_usd DESC
        ) AS rank_per_month,

        NTILE(5) OVER (
            PARTITION BY usage_month
            ORDER BY estimated_monthly_waste_usd DESC
        ) AS waste_quintile

    FROM monthly_summary
)

SELECT
    *,
    
    CASE
        WHEN waste_quintile = 1
             AND avg_monthly_waste_ratio > 0.30
        THEN TRUE
        ELSE FALSE
    END AS requires_immediate_action

FROM ranked_distribution
WHERE rank_per_month <= 10
ORDER BY usage_month DESC, rank_per_month;