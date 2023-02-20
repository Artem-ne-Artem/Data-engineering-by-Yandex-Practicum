CREATE TABLE IF NOT EXISTS de.cdm.dm_courier_ledger
(
	id	serial,
	courier_id	integer,
	courier_name	varchar,
	settlement_year integer not null,
    settlement_month integer not null,
	orders_count	integer,
	orders_total_sum	numeric(14, 2),
	rate_avg	NUMERIC,
	order_processing_fee	NUMERIC,
	courier_order_sum	numeric(14, 2),
	courier_tips_sum	numeric(14, 2),
	courier_reward_sum	numeric(14, 2),
    CONSTRAINT dm_courier_settlement_report_pk primary key (id),
	CONSTRAINT dm_courier_settlement_report_courier_unique UNIQUE (courier_id, settlement_year, settlement_month),
	CONSTRAINT dm_courier_settlement_report_orders_count_check check (orders_count >= 0),
	CONSTRAINT dm_courier_settlement_report_orders_total_sum_check check (orders_total_sum >= 0),
	CONSTRAINT dm_courier_settlement_report_rate_avg_check check (rate_avg >= 0),
	CONSTRAINT dm_courier_settlement_report_order_processing_fee_check check (order_processing_fee >= 0),
	CONSTRAINT dm_courier_settlement_report_courier_order_sum_check check (courier_order_sum >= 0),
	CONSTRAINT dm_courier_settlement_report_courier_tips_sum_check check (courier_tips_sum >= 0),
	CONSTRAINT dm_courier_settlement_report_courier_reward_sum_check check (courier_reward_sum >= 0)
);
