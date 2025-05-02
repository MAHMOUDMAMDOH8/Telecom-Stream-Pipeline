
  create or replace   view STOCK.public_silver.Dim_companies
  
   as (
    

with Dim_companies as (
    select
        *
    from STOCK.SILVER.companies
)

select * from Dim_companies
  );

