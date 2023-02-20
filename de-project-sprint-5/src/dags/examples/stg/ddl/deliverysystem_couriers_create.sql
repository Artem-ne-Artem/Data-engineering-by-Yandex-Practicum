CREATE TABLE IF NOT EXISTS de.stg.deliverysystem_couriers
(
    id serial NOT NULL,
    object_id varchar NOT NULL,
    update_ts timestamp default now() NOT NULL,
    object_value varchar NOT NULL,
    CONSTRAINT stg_deliverysystem_couriers_pk PRIMARY KEY (id),
    CONSTRAINT stg_deliverysystem_couriers_object_id_uindex UNIQUE (object_id)
);