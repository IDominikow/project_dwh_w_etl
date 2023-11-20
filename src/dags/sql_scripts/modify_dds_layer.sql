CREATE TABLE dds.dm_couriers (
	id serial4 NOT NULL,
	courier_id text NOT NULL,
	courier_name text NOT NULL,
	CONSTRAINT dm_couriers_pkey PRIMARY KEY (id),
	CONSTRAINT dm_couriers_unique_key UNIQUE (courier_id)
);

CREATE TABLE dds.dm_orders (
	id serial4 NOT NULL,
	order_key varchar NOT NULL,
	user_id int4 NOT NULL,
	restaurant_id int4 NOT NULL,
	courier_id int4 NULL,
	CONSTRAINT dm_orders_order_key_uindex UNIQUE (order_key),
	CONSTRAINT dm_orders_pkey PRIMARY KEY (id),
	CONSTRAINT dm_orders_courier_id_fkey FOREIGN KEY (courier_id) REFERENCES dds.dm_couriers(id),
	CONSTRAINT dm_orders_restaurant_id_fkey FOREIGN KEY (restaurant_id) REFERENCES dds.dm_restaurants(id),
	CONSTRAINT dm_orders_user_id_fkey FOREIGN KEY (user_id) REFERENCES dds.dm_users(id)
);

CREATE TABLE dds.fct_order_deliveries (
	id serial4 NOT NULL,
	order_id int4 NOT NULL,
	order_sum numeric(15, 9) NOT NULL,
	delivery_rate numeric(2, 1) NOT NULL,
	delivery_tip_sum numeric(15, 9) NOT NULL,
	CONSTRAINT fct_order_deliveries_delivery_rate_check CHECK (((delivery_rate >= (0)::numeric) AND (delivery_rate <= (5)::numeric))),
	CONSTRAINT fct_order_deliveries_delivery_tip_sum_check CHECK ((delivery_tip_sum >= (0)::numeric)),
	CONSTRAINT fct_order_deliveries_order_sum_check CHECK ((order_sum >= (0)::numeric)),
	CONSTRAINT fct_order_deliveries_pkey PRIMARY KEY (id),
	CONSTRAINT fct_order_deliveries_order_id_fkey FOREIGN KEY (order_id) REFERENCES dds.dm_orders(id)
);

CREATE TABLE dds.dm_statuses (
	id int4 NOT NULL DEFAULT nextval('dds.dm_order_statuses_id_seq'::regclass),
	status_name text NOT NULL,
	CONSTRAINT dm_statuses_unique_key UNIQUE (status_name),
	CONSTRAINT order_statuses_pkey PRIMARY KEY (id)
);

CREATE TABLE dds.dm_order_statuses (
	id int4 NOT NULL DEFAULT nextval('dds.dm_order_status_timestamps_id_seq'::regclass),
	order_id int4 NOT NULL,
	status_id int4 NOT NULL,
	timestamp_id int4 NOT NULL,
	CONSTRAINT dm_order_status_timestamps_pkey PRIMARY KEY (id),
	CONSTRAINT dm_order_status_timestamps_order_id_fkey FOREIGN KEY (order_id) REFERENCES dds.dm_orders(id),
	CONSTRAINT dm_order_status_timestamps_status_id_fkey FOREIGN KEY (status_id) REFERENCES dds.dm_statuses(id),
	CONSTRAINT dm_order_status_timestamps_timestamp_id_fkey FOREIGN KEY (timestamp_id) REFERENCES dds.dm_timestamps(id)
);