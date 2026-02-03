select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with all_values as (

    select
        price_bucket as value_field,
        count(*) as n_records

    from silver.silver_ecommerce_events
    group by price_bucket

)

select *
from all_values
where value_field not in (
    'UNKNOWN','0-50','50-100','100-200','200-500','500+'
)



      
    ) dbt_internal_test