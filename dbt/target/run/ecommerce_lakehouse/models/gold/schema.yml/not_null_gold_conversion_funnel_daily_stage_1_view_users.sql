select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select stage_1_view_users
from silver_gold.gold_conversion_funnel_daily
where stage_1_view_users is null



      
    ) dbt_internal_test