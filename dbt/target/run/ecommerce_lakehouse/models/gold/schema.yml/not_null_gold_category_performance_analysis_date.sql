select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select analysis_date
from silver_gold.gold_category_performance
where analysis_date is null



      
    ) dbt_internal_test