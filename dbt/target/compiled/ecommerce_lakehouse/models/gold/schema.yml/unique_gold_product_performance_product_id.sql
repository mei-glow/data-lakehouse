
    
    

select
    product_id as unique_field,
    count(*) as n_records

from silver_gold.gold_product_performance
where product_id is not null
group by product_id
having count(*) > 1


