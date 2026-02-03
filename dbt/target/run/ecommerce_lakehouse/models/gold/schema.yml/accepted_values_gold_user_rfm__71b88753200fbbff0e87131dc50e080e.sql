select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with all_values as (

    select
        rfm_segment as value_field,
        count(*) as n_records

    from silver_gold.gold_user_rfm_segments
    group by rfm_segment

)

select *
from all_values
where value_field not in (
    'Champions','Loyal Customers','Potential Loyalists','At Risk','Cannot Lose Them','Hibernating','Lost','New Customers'
)



      
    ) dbt_internal_test