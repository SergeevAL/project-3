INSERT INTO mart.f_sales (date_id, item_id, customer_id, city_id, quantity, payment_amount)
SELECT distinct dc.date_id, item_id, customer_id, city_id, quantity, payment_amount 
FROM staging.user_orders_log uol 
left JOIN mart.d_calendar AS dc ON uol.date_time::Date = dc.date_actual
ON CONFLICT (date_id, item_id, customer_id, city_id, quantity, payment_amount) 
DO nothing;
