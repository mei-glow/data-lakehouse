
    
    

with all_values as (

    select
        event_type as value_field,
        count(*) as n_records

    from silver.silver_ecommerce_events
    group by event_type

)

select *
from all_values
where value_field not in (
    'view','cart','purchase','remove_from_cart'
)


