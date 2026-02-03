select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select event_hour
from silver_gold.gold_hourly_traffic
where event_hour is null



      
    ) dbt_internal_test