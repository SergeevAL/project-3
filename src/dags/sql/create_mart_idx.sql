CREATE INDEX d_date_date_actual_idx ON mart.d_calendar USING btree (date_actual);
CREATE INDEX d_city1 ON mart.d_city USING btree (city_id);
CREATE INDEX d_cust1 ON mart.d_customer USING btree (customer_id);
CREATE UNIQUE INDEX d_item1 ON mart.d_item USING btree (item_id);
CREATE INDEX f_ds1 ON mart.f_sales USING btree (date_id);
CREATE INDEX f_ds2 ON mart.f_sales USING btree (item_id);
CREATE INDEX f_ds3 ON mart.f_sales USING btree (customer_id);
CREATE INDEX f_ds4 ON mart.f_sales USING btree (city_id);
CREATE INDEX f_sd_idx ON mart.f_customer_retention USING btree (start_date);
