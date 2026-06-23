When using window functions usually you will use RANGE or ROWS.

Performance:
Note that Snowflake use a different processing for RANGE and ROWS.
Using ROWS will always be more performant than using RANGE, so always use ROWS when you can.

RANGE is mainly use for a fixed time ranged while ROWS is mainly used to calculate running totals.

ROWS doesn't handle duplicates, that means if your order_by is by sale_date for example; 
Your toto row may be calculated before your titi row, and if you re-execute the exact same query your titi row might be executed before your toto row. That's why it is recommended to use order_by on 2 columns to avoid the duplicate and having indempotent workflow ;

SELECT 
    SALE_DATE,
    -- 1. Extract month
    DATE_TRUNC('month', SALE_DATE) AS SALE_MONTH,
    AMOUNT,
    -- 2. Running total month
    SUM(AMOUNT) OVER (
        PARTITION BY DATE_TRUNC('month', SALE_DATE) -- Réinitialize the running total by month
        ORDER BY SALE_DATE ASC, SALE_ID ASC         -- Avoid duplicates
        ROWS UNBOUNDED PRECEDING                    -- Max Performance (not using range)
    ) AS CUMULATIVE_MONTHLY_SALES
FROM SALES
ORDER BY SALE_DATE ASC;


Also regarding the default, if you do that : 

SUM(AMOUNT) OVER (PARTITION BY CUSTOMER_ID ORDER BY SALE_DATE)

By default snowflake will apply the following rule:
SUM(AMOUNT) OVER (PARTITION BY CUSTOMER_ID ORDER BY SALE_DATE ASC RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)

Which is bad because by default you will use the RANGE processing. So always add ROWS UNBOUNDED PRECEDING after your order_by to optimise your query:

SUM(AMOUNT) OVER (PARTITION BY CUSTOMER_ID ORDER BY SALE_DATE ASC ROWS UNBOUNDED PRECEDING)