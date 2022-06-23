INSERT INTO mart.f_customer_retention
WITH regional_sales AS (
    SELECT dc.date_actual, fs.customer_id, status ,count(*) retention, sum(fs.payment_amount) payment_amount
    FROM mart.f_sales fs 
    JOIN (SELECT date_id, date_actual 
         FROM mart.d_calendar
			WHERE day_of_week = 1) dc 
	USING (date_id)
	GROUP BY dc.date_actual, fs.customer_id, status
   )
SELECT rs.date_actual,
   'weekly' period_name,
   COUNT(CASE WHEN rs.retention = 1 THEN 1 END) AS new_customers_count,
   COUNT(CASE WHEN rs.retention > 1 AND rs.status = 'shipped' THEN 1 END) AS returning_customers_count,
   COUNT(CASE WHEN rs.status = 'refunded' THEN 1 END) AS refunded_customers_count,
   SUM(CASE WHEN rs.retention = 1 THEN payment_amount END) AS new_customers_revenue,
   SUM(CASE WHEN rs.retention > 1 AND rs.status = 'shipped' THEN payment_amount END) AS returning_customers_revenue,
   SUM(CASE WHEN rs.status = 'refunded' THEN payment_amount else 0 END) AS refunded_customers_revenue
FROM regional_sales rs
JOIN mart.d_customer c
USING (customer_id)
GROUP BY date_actual
ON CONFLICT (start_date,period_name) DO UPDATE SET (new_customers_count, returning_customers_count, refunded_customers_count, new_customers_revenue, returning_customers_revenue, refunded_customers_revenue) = (excluded.new_customers_count, excluded.returning_customers_count, excluded.refunded_customers_count, excluded.new_customers_revenue, excluded.returning_customers_revenue, excluded.refunded_customers_revenue);
