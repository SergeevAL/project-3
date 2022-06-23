DROP SCHEMA if exists mart cascade;

-- DROP TABLE mart.d_calendar;
CREATE SCHEMA if not exists mart;

CREATE TABLE mart.d_calendar (
	date_id serial PRIMARY KEY,
	date_actual date NOT NULL,
	epoch int8 NOT NULL,
	day_suffix varchar(4) NOT NULL,
	day_name varchar(9) NOT NULL,
	day_of_week int4 NOT NULL,
	day_of_month int4 NOT NULL,
	day_of_quarter int4 NOT NULL,
	day_of_year int4 NOT NULL,
	week_of_month int4 NOT NULL,
	week_of_year int4 NOT NULL,
	week_of_year_iso bpchar(10) NOT NULL,
	month_actual int4 NOT NULL,
	month_name varchar(9) NOT NULL,
	month_name_abbreviated bpchar(3) NOT NULL,
	quarter_actual int4 NOT NULL,
	quarter_name varchar(9) NOT NULL,
	year_actual int4 NOT NULL,
	first_day_of_week date NOT NULL,
	last_day_of_week date NOT NULL,
	first_day_of_month date NOT NULL,
	last_day_of_month date NOT NULL,
	first_day_of_quarter date NOT NULL,
	last_day_of_quarter date NOT NULL,
	first_day_of_year date NOT NULL,
	last_day_of_year date NOT NULL,
	mmyyyy bpchar(6) NOT NULL,
	mmddyyyy bpchar(10) NOT NULL,
	weekend_indr bool NOT NULL
);
--CREATE INDEX d_date_date_actual_idx ON mart.d_calendar USING btree (date_actual);

CREATE TABLE mart.d_city (
	id serial4 PRIMARY KEY,
	city_id int4 NULL,
	city_name varchar(50) NULL,
	CONSTRAINT d_city_city_id_key UNIQUE (city_id)
);
--CREATE INDEX d_city1 ON mart.d_city USING btree (city_id);

-- DROP TABLE mart.d_customer;

CREATE TABLE mart.d_customer (
	id serial4 PRIMARY KEY,
	customer_id int4 NOT NULL,
	first_name varchar(15) NULL,
	last_name varchar(15) NULL,
	city_id int4 NULL,
	CONSTRAINT d_customer_customer_id_key UNIQUE (customer_id)
);
--CREATE INDEX d_cust1 ON mart.d_customer USING btree (customer_id);


-- DROP TABLE mart.d_item;

CREATE TABLE mart.d_item (
	id serial4 PRIMARY KEY,
	item_id int4 NOT NULL,
	item_name varchar(50) NULL,
	CONSTRAINT d_item_item_id_key UNIQUE (item_id)
);
--CREATE UNIQUE INDEX d_item1 ON mart.d_item USING btree (item_id);


--DROP TABLE if exists mart.f_sales;

CREATE TABLE mart.f_sales (
	id serial PRIMARY KEY,
	date_id bigint NOT NULL,
	item_id bigint NOT NULL,
	customer_id bigint NOT NULL,
	city_id bigint NOT NULL,
	quantity bigint NULL,
	payment_amount numeric(10, 2) NULL,
	status varchar(20) NOT NULL DEFAULT 'shipped',
    CONSTRAINT f_sales_id_key UNIQUE (date_id, item_id, customer_id, city_id, quantity, payment_amount),
	CONSTRAINT f_sales_customer_id_fkey FOREIGN KEY (customer_id) REFERENCES mart.d_customer(customer_id),
	CONSTRAINT f_sales_date_id_fkey FOREIGN KEY (date_id) REFERENCES mart.d_calendar(date_id),
	CONSTRAINT f_sales_item_id_fkey FOREIGN KEY (item_id) REFERENCES mart.d_item(item_id)
);
--CREATE INDEX f_ds1 ON mart.f_sales USING btree (date_id);
--CREATE INDEX f_ds2 ON mart.f_sales USING btree (item_id);
--CREATE INDEX f_ds3 ON mart.f_sales USING btree (customer_id);
--CREATE INDEX f_ds4 ON mart.f_sales USING btree (city_id);

--DROP TABLE if exists mart.f_customer_retention;

CREATE TABLE mart.f_customer_retention (
	start_date date NOT NULL,
	period_name varchar(20) NOT NULL,
	new_customers_count bigint NULL,
	returning_customers_count bigint NULL,
	refunded_customers_count bigint NULL,
	new_customers_revenue bigint NULL,
	returning_customers_revenue bigint NULL,
	refunded_customers_revenue bigint NULL,
	PRIMARY KEY (start_date, period_name)
);
--CREATE INDEX f_sd_idx ON mart.f_customer_retention USING btree (start_date);

--DROP TABLE if exists mart.load_history;

CREATE TABLE mart.load_history (
	id serial PRIMARY KEY,
    datetime timestamp NOT NULL DEFAULT NOW(),
    proc_name varchar(100) NOT NULL DEFAULT 'Upload',
	target_table varchar(100),
    source_table varchar(100),
  	duration_ms bigint,
	rows bigint,
  	status varchar(100) NOT NULL DEFAULT 'Success',
  	msg varchar(1000), 
  	target_date date
);

