

with category_cte as (
    select  
        id as category_id , 
        cat as category ,
        subcat as sub_category,
        maintenance 
    from  sales.bronze_layer.erp_px_cat_g1v2
)
select * from category_cte