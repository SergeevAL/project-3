CREATE INDEX ual1 ON staging.user_activity_log USING btree (customer_id);
CREATE INDEX uo1 ON staging.user_order_log USING btree (customer_id);
CREATE INDEX uo2 ON staging.user_order_log USING btree (item_id);
CREATE INDEX pl1 ON staging.price_log USING btree (category_id);
CREATE INDEX pl2 ON staging.price_log USING btree (item_id);
