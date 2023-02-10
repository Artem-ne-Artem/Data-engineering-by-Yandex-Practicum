select		current_timestamp as test_date_time
			,'test_01' as test_name
			,case when count(1) = 0 then false else true end as test_result
from		(select		dsra.id as dsra_id
						,dsra.restaurant_id as dsra_restaurant_id
						,dsra.restaurant_name as dsra_restaurant_name
						,dsra.settlement_year as dsra_settlement_year
						,dsra.settlement_month as dsra_settlement_month
						,dsra.orders_count as dsra_orders_count
						,dsra.orders_total_sum as dsra_orders_total_sum
						,dsra.orders_bonus_payment_sum as dsra_orders_bonus_payment_sum
						,dsra.orders_bonus_granted_sum as dsra_orders_bonus_granted_sum
						,dsra.order_processing_fee as dsra_order_processing_fee
						,dsra.restaurant_reward_sum as dsra_restaurant_reward_sum
						,dsre.id as dsre_id
						,dsre.restaurant_id as dsre_restaurant_id
						,dsre.restaurant_name as dsre_restaurant_name
						,dsre.settlement_year as dsre_settlement_year
						,dsre.settlement_month as dsre_settlement_month
						,dsre.orders_count as dsre_orders_count
						,dsre.orders_total_sum as dsre_orders_total_sum
						,dsre.orders_bonus_payment_sum as dsre_orders_bonus_payment_sum
						,dsre.orders_bonus_granted_sum as dsre_orders_bonus_granted_sum
						,dsre.order_processing_fee as dsre_order_processing_fee
						,dsre.restaurant_reward_sum as dsre_restaurant_reward_sum
			from		de.public_test.dm_settlement_report_actual as dsra
			full join	de.public_test.dm_settlement_report_expected as dsre
						on dsra.id = dsre.id
			) as tbl
where		dsra_id is null or dsre_id is null