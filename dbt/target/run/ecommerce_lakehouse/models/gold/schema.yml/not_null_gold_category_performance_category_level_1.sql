select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select category_level_1
from silver_gold.gold_category_performance
where category_level_1 is null



      
    ) dbt_internal_test