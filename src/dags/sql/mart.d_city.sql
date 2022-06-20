INSERT INTO mart.d_city (city_id, city_name)
SELECT distinct city_id, city_name FROM staging.user_orders_log
ON CONFLICT (city_id) DO UPDATE SET city_name = excluded.city_name;
