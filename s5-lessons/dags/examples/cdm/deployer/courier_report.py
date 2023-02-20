from lib import PgConnect


class CourierLedgerRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def load_ledger_by_month(self) -> None:
        with self._db.client() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
INSERT INTO de.cdm.dm_courier_ledger (courier_id, courier_name, settlement_year, settlement_month, orders_count, orders_total_sum, rate_avg, order_processing_fee, courier_order_sum, courier_tips_sum, courier_reward_sum)
WITH 
tmp_report AS 
(
SELECT
			fct.courier_id,
			dc."name" AS courier_name,
			EXTRACT('YEAR' FROM fct.order_ts) AS settlement_year,
			EXTRACT('MONTH' FROM fct.order_ts) AS settlement_month,
			COUNT(fct.order_id) AS orders_count,
			SUM(fct.total_sum) AS orders_total_sum,
			AVG(fct.rate) AS rate_avg,
			SUM(fct.total_sum) * 0.25 AS order_processing_fee,
			CASE
			    WHEN AVG(fct.rate) < 4 THEN SUM(fct.total_sum) * 0.05
			    WHEN AVG(fct.rate) <= 4 OR AVG(fct.rate) < 4.5 THEN SUM(fct.total_sum) * 0.07
			    WHEN AVG(fct.rate) <= 4.5 OR AVG(fct.rate) < 4.9 THEN SUM(fct.total_sum) * 0.08
			    WHEN AVG(fct.rate) >= 4.9 THEN SUM(fct.total_sum) * 0.1
			END AS tmp_courier_order_sum,
			SUM(fct.tip_sum) AS courier_tips_sum
FROM 		de.dds.fct_deliveries AS fct
LEFT JOIN	de.dds.dm_couriers AS dc 
			ON dc.id = fct.courier_id 
GROUP BY 	1, 2, 3, 4
)
SELECT 
			courier_id,
			courier_name,
			settlement_year,
			settlement_month,
			orders_count,
			orders_total_sum,
			rate_avg,
			order_processing_fee,
			CASE 
		        WHEN rate_avg < 4 AND tmp_courier_order_sum < 100 THEN 100
		        WHEN (rate_avg <= 4 OR rate_avg < 4.5) AND tmp_courier_order_sum < 150 THEN 150
		        WHEN (rate_avg <= 4.5 OR rate_avg < 4.9) AND tmp_courier_order_sum < 175 THEN 175
		        WHEN rate_avg >= 4.9 AND tmp_courier_order_sum < 200 THEN 200 
		        ELSE tmp_courier_order_sum
		    END AS courier_order_sum,
			courier_tips_sum,
			tmp_courier_order_sum + courier_tips_sum * 0.95 AS courier_reward_sum
FROM 		tmp_report
ON CONFLICT (courier_id, settlement_year, settlement_month) DO UPDATE
SET
    orders_count = EXCLUDED.orders_count,
    orders_total_sum = EXCLUDED.orders_total_sum,
    rate_avg = EXCLUDED.rate_avg,
    order_processing_fee = EXCLUDED.order_processing_fee,
    courier_order_sum = EXCLUDED.courier_order_sum,
    courier_tips_sum = EXCLUDED.courier_tips_sum,
    courier_reward_sum = EXCLUDED.courier_reward_sum;
;
                    """
                )
                conn.commit()


class CourierLedgerLoader:

    def __init__(self, pg: PgConnect) -> None:
        self.repository = CourierLedgerRepository(pg)

    def load_report_by_month(self):
        self.repository.load_ledger_by_month()
