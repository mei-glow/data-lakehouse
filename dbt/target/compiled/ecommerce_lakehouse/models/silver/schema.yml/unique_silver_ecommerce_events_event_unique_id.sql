
    
    

select
    event_unique_id as unique_field,
    count(*) as n_records

from silver.silver_ecommerce_events
where event_unique_id is not null
group by event_unique_id
having count(*) > 1


