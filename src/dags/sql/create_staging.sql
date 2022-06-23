drop schema if exists staging cascade;

-- drop table if exists staging.events_log;
-- drop table if exists staging.customer_research;
-- drop table if exists staging.user_activity_log;
-- drop table if exists staging.user_order_log;
-- drop table if exists staging.price_log;

create schema if not exists staging;

create table if not exists staging.events_log (
	id serial PRIMARY KEY,
  	datetime timestamp NOT NULL DEFAULT NOW(),
  	proc_name varchar(100) NOT NULL DEFAULT 'Upload',
  	target_table varchar(100),
  	source_file varchar(1000),
  	duration_ms int,
	rows bigint,
  	status varchar(100) NOT NULL DEFAULT 'Success',
  	msg varchar(1000), 
  	target_date date
);

create table if not exists staging.customer_research (
	id serial PRIMARY KEY,
	date_id timestamp NOT NULL,
	category_id int NOT NULL,
	geo_id int NOT NULL,
	sales_qty int,
	sales_amt numeric(14,2)
);

create table if not exists staging.user_activity_log (
	id serial PRIMARY KEY,
	date_time timestamp NOT NULL,
	action_id bigint NOT NULL,
	customer_id bigint NOT NULL,
	quantity int8
);
--CREATE INDEX ual1 ON staging.user_activity_log USING btree (customer_id);

create table if not exists staging.user_order_log (
	id serial PRIMARY KEY,
	date_time timestamp NOT NULL,
	city_id int4 NOT NULL,
	city_name varchar(100) NULL,
	customer_id int4 NOT NULL,
	first_name varchar(100) NULL,
	last_name varchar(100) NULL,
	item_id int4 NOT NULL,
	item_name varchar(100) NULL,
	quantity int8 NULL,
	payment_amount numeric(10, 2) NULL
);
--CREATE INDEX uo1 ON staging.user_order_log USING btree (customer_id);
--CREATE INDEX uo2 ON staging.user_order_log USING btree (item_id);

create table if not exists staging.price_log (
	id serial PRIMARY KEY,
  	datetime timestamp NOT NULL,
  	category_id int NOT NULL,
  	category_name varchar(100) NOT NULL,
  	item_id int NOT NULL,
  	price int,
  	batch_id int
);
--CREATE INDEX pl1 ON staging.price_log USING btree (category_id);
--CREATE INDEX pl2 ON staging.price_log USING btree (item_id);
