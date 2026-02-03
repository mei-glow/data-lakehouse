
    
    

with all_values as (

    select
        current_funnel_stage as value_field,
        count(*) as n_records

    from silver_gold.gold_user_journey_funnel
    group by current_funnel_stage

)

select *
from all_values
where value_field not in (
    'viewer_only','cart_abandoner','purchaser','repeat_buyer','unknown'
)


