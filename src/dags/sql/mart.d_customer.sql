INSERT INTO mart.d_customer (customer_id, first_name, last_name, city_id)
SELECT customer_id, first_name, last_name, max(city_id) city_id
FROM staging.user_orders_log
GROUP BY customer_id, first_name, last_name
ON CONFLICT (customer_id) DO UPDATE SET (first_name, last_name, city_id) = (excluded.first_name, excluded.last_name, excluded.city_id);
