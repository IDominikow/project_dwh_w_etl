CREATE TABLE stg.delivery_system_couriers (
	id serial4 NOT NULL,
	object_id text NOT NULL,
	object_value json NOT NULL,
	upload_ts timestamp NOT NULL,
	CONSTRAINT delivery_system_couriers_pkey PRIMARY KEY (id)
);

CREATE TABLE stg.delivery_system_deliveries (
	id serial4 NOT NULL,
	object_id text NOT NULL,
	object_value json NOT NULL,
	upload_ts timestamp NOT NULL,
	CONSTRAINT delivery_system_deliveries_pkey PRIMARY KEY (id)
);