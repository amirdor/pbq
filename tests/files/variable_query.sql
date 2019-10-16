select date(created_at), count(*) as count_orders
from `dwh.orders`
where _PARTITIONTIME >= Timestamp(date_sub(current_date("UTC"), INTERVAL {until_day} DAY))
group by 1
