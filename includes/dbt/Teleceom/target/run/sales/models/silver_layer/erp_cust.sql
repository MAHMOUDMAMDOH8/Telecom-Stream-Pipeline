
  
    

        create or replace transient table sales.layer_silver.erp_cust
         as
        (

with erp_customer as (
    SELECT 
        CASE 
            WHEN cid LIKE 'NAS%' THEN SUBSTR(cid, 4, LENGTH(cid)) 
            ELSE cid 
        END AS customer_id, 
        
        CASE 
            WHEN bdate > GETDATE() THEN NULL 
            ELSE bdate 
        END AS birth_date,  
        
        CASE 
            WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
            WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
            ELSE 'n/a'
        END AS Gender
    FROM sales.bronze_layer.erp_cust_az12
)
SELECT * FROM erp_customer
        );
      
  