Constraints set on DDL or DML later: 

-- Examples :
CREATE OR REPLACE TABLE ECOM.SALES (
    sale_id STRING,
    amount NUMBER,
    CONSTRAINT chk_positive_amount CHECK (amount > 0) 
);

-- Add later
ALTER TABLE ECOM.SALES 
ADD CONSTRAINT chk_positive_amount CHECK (amount > 0);

-- NOT ENFORCED
By default the constraint is NOT ENFORCED which means the lines that don't match are still inserted.
You gain query optimization ( scan ).

SELECT * FROM ECOM.LOYALTY.SALES WHERE amount = -50; 
This request will cost nothing because the constraint explicitly doesn't allow minus amount. ( but your data quality might not be there as you're still letting minus amount to be inserted)

-- ENFORCED
If the constraint is ENFORCED which means the lines that don't match are not inserted. You must configure it :
ALTER TABLE ECOM.SALES ALTER CONSTRAINT chk_positive_amount ENFORCED;

The whole insert is cancelled and an ERROR is received

-- ENFORCED + ERROR_ON_VIOLATION = FALSE

By having both activated , ENFORCED at your DDL and ERROR_ON_VIOLATION = FALSE on your insert :

INSERT INTO ECOM.SALES 
SELECT * FROM ECOM.SALES_RAW
WITH ERROR_ON_VIOLATION = FALSE;

The non matching row will be put in the ERROR_TABLE('ECOM.SALES')