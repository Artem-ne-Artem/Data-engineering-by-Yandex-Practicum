CREATE TABLE IF NOT EXISTS de.dds.fct_deliveries(
    id serial,
	delivery_id	varchar NOT NULL,
	courier_id	integer NOT NULL,
	order_id integer NOT NULL,
	order_ts timestamp NOT NULL,
	delivery_ts timestamp NOT NULL,
	address varchar NOT NULL,
	rate  integer NOT NULL,
	tip_sum numeric (14, 2) NOT NULL,
	total_sum numeric (14, 2) NOT NULL,
    CONSTRAINT dds_fct_deliveries_id_pk PRIMARY KEY (id),
    CONSTRAINT dds_fct_deliveries_order_id_dwh_fk FOREIGN KEY (order_id) REFERENCES de.dds.dm_orders(id),
    CONSTRAINT dds_fct_deliveries_courier_id_dwh_fkey FOREIGN KEY (courier_id) REFERENCES de.dds.dm_couriers(id)
);