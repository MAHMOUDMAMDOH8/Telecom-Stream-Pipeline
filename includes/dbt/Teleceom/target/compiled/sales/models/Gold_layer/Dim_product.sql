

WITH Dim_product AS (
    SELECT 
        p.product_id,
        p.product_key AS product_number,
        p.product_name,
        cat.category,
        cat.sub_category,
        cat.maintenance,
        p.product_cost,
        p.product_line,
        p.start_date
    FROM sales.layer_silver.crm_prd_info  AS p
    LEFT JOIN sales.layer_silver.ERP_PX_CAT AS cat 
        ON p.category_id = cat.category_id
)
SELECT * FROM Dim_product