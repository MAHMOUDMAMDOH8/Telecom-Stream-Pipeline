

with Dim_customer as(
    SELECT 
        DISTINCT cust.id AS customer_id,
        cust.customer_key AS customer_unique_key,
        cust.first_name,
        cust.last_name,
        CASE 
            WHEN cust.gender ='n/a' THEN erp.gender
            ELSE cust.gender
        END AS gender,
        erp.birth_date,
        loc.country
    FROM sales.layer_silver.crm_cust_info AS cust
    LEFT JOIN sales.layer_silver.erp_cust AS erp 
        ON cust.customer_key = erp.customer_id
    LEFT JOIN sales.layer_silver.erp_customer_loc  AS loc 
        ON cust.customer_key = loc.customer_id
)
select * from Dim_customer