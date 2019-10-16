select date(created_at), count(*) as count_orders
from `dwh.orders`
where _PARTITIONTIME >= Timestamp(date_sub(current_date('UTC'), INTERVAL 1 DAY))
group by 1
