select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select event_unique_id
from silver.silver_ecommerce_events
where event_unique_id is null



      
    ) dbt_internal_test