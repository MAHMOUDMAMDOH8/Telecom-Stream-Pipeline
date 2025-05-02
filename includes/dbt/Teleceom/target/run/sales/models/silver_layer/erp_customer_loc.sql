
  
    

        create or replace transient table sales.layer_silver.erp_customer_loc
         as
        (

with loaction_cte as (
    select 
        replace(cid,'-','') as customer_id,
        case 
            when trim(cntry) = 'DE' then 'Germany'
            when trim(cntry) in ('US','USA') then 'United States'
            when trim(cntry) ='' or  cntry is null then 'n/a'
            else trim(cntry)
        end as country
    from sales.bronze_layer.erp_loc_a101  
)
SELECT * from loaction_cte
        );
      
  