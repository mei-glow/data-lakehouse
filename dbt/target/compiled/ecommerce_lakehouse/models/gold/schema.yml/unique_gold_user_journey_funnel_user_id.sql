
    
    

select
    user_id as unique_field,
    count(*) as n_records

from silver_gold.gold_user_journey_funnel
where user_id is not null
group by user_id
having count(*) > 1


