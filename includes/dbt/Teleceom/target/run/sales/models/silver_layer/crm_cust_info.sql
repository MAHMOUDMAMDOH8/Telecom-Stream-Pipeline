
  
    

        create or replace transient table sales.layer_silver.crm_cust_info
         as
        (

with customer_cte as (
    SELECT 
        *, 
        row_number() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS last_update
    FROM sales.bronze_layer.crm_cust_info
)

SELECT 
    cst_id AS ID,
    cst_key AS customer_key, 
    TRIM(cst_firstname) AS FIRST_NAME, 
    TRIM(cst_lastname) AS LAST_NAME,
    CASE 
        WHEN UPPER(cst_marital_status) = 'S' THEN 'Single'
        WHEN UPPER(cst_marital_status) = 'M' THEN 'Married'
        ELSE 'n/a'
    END AS MARITAL_STATUS,
    CASE 
        WHEN UPPER(cst_gndr) = 'F' THEN 'Female'
        WHEN UPPER(cst_gndr) = 'M' THEN 'Male'
        ELSE 'n/a'
    END AS gender,
    cst_create_date 
FROM customer_cte
WHERE last_update = 1 and cst_id is not null
        );
      
  