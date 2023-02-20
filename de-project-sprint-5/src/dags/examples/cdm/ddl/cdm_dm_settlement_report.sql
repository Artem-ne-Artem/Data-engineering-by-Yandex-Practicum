CREATE TABLE IF NOT EXISTS cdm.dm_settlement_report (
    id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    restaurant_id VARCHAR NOT NULL,
    restaurant_name VARCHAR NOT NULL,
    settlement_date DATE NOT NULL CHECK(settlement_date > '2022-01-01' AND settlement_date < '2050-01-01'),
    orders_count int NOT NULL CHECK(orders_count >= 0),
    orders_total_sum numeric(14, 2) NOT NULL DEFAULT 0 CHECK (orders_total_sum >= 0),
    orders_bonus_payment_sum numeric(14, 2) NOT NULL DEFAULT 0 CHECK (orders_bonus_payment_sum >= 0),
    orders_bonus_granted_sum numeric(14, 2) NOT NULL DEFAULT 0 CHECK (orders_bonus_granted_sum >= 0),
    order_processing_fee numeric(14, 2) NOT NULL DEFAULT 0 CHECK (order_processing_fee >= 0),
    restaurant_reward_sum numeric(14, 2) NOT NULL DEFAULT 0 CHECK (restaurant_reward_sum >= 0),
    UNIQUE(restaurant_id, settlement_date)
);

DO $do$ BEGIN IF EXISTS (
    SELECT
    FROM pg_catalog.pg_roles
    WHERE rolname = 'sp5_de_tester'
) THEN
GRANT SELECT ON all tables IN SCHEMA cdm TO sp5_de_tester;
END IF;
END $do$;