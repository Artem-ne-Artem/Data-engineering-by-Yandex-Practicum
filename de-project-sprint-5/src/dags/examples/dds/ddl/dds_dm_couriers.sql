CREATE TABLE IF NOT EXISTS de.dds.dm_couriers(
	id serial,
	courier_id varchar NOT null,
	name varchar NOT null,
    CONSTRAINT dds_dm_couriers_id_pk PRIMARY KEY (id),
    CONSTRAINT dds_dm_couriers_courier_id_unique UNIQUE (courier_id)
);
