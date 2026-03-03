Applied at a Column level

Scan less

Recommended to use on massive tables where you want a small result

Storage Cost: Creates additional metadata structures that consume storage space

Maintenance Cost: Background serverless credits are consumed every time the base table is updated

Can be used on EQUALITY or SUBSTRING

EXAMPLES :

ALTER TABLE large_orders 
ADD SEARCH OPTIMIZATION ON EQUALITY(customer_name, order_id);

ALTER TABLE logs 
ADD SEARCH OPTIMIZATION ON SUBSTRING(error_message);