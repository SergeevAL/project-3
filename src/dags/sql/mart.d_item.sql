insert into mart.d_item (item_id,item_name)
SELECT DISTINCT
  item_id,first_value(item_name) OVER (PARTITION BY item_id ORDER BY date_time DESC) item_name
FROM staging.user_orders_log
ON CONFLICT (item_id) DO UPDATE SET item_name = excluded.item_name;
