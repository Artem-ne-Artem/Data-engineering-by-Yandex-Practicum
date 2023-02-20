## Слой CDM

#### Витрина dm_courier_ledger
* id							идентификатор записи
* courier_id					сквозной идентификатор курьера
* courier_name					Ф. И. О. курьера
* settlement_year				год отчёта
* settlement_month				месяц отчёта, где 1 — январь и 12 — декабрь
* orders_count					количество заказов за период (месяц)
* orders_total_sum				общая стоимость заказов
* rate_avg						средний рейтинг курьера по оценкам пользователей
* order_processing_fee			сумма, удержанная компанией за обработку заказов, которая высчитывается как orders_total_sum * 0.25
* courier_order_sum				сумма, которую необходимо перечислить курьеру за доставленные им/ей заказы. За каждый доставленный заказ курьер должен получить некоторую сумму в зависимости от рейтинга (см. ниже)
* courier_tips_sum				сумма, которую пользователи оставили курьеру в качестве чаевых
* courier_reward_sum			сумма, которую необходимо перечислить курьеру. Вычисляется как courier_order_sum + courier_tips_sum * 0.95 (5% — комиссия за обработку платежа)

#### Витрина dm_settlement_report
* id							идентификатор записи
* restaurant_id					идентификатор ресторана
* restaurant_name				наименование ресторана
* settlement_date				год отчёта
* orders_count 					количество заказов за период (месяц)
* orders_total_sum				общая сумма заказов, т.е. сумма платежей клиентов
* orders_bonus_payment_sum		сумма оплат бонусами за период
* orders_bonus_granted_sum		сумма накопленных бонусов за период
* order_processing_fee			сумма, удержанная компанией за обработку заказов. Высчитывается как orders_total_sum * 0.25
* restaurant_reward_sum			сумма, которую необходимо перечислить ресторану. total_sum - orders_bonus_payment_sum - order_processing_fee

## Слой DDS

#### Перечень таблиц необходимых для построения витрин:
* dm_couriers					id, name
* dm_orders						id, timestamp_id
* fct_deliveries				order_id, courier_id, rate, total_sum, tip_sum

## Слой STG

#### Источник PostgreSQL (система бонусов)
* bonussystem_users
* bonussystem_ranks
* bonussystem_events

#### Источник MongoDB (система заказов)
* ordersystem_users
* ordersystem_restaurants
* ordersystem_orders

#### Источник API (система доствки)
* deliverysystem_couriers
* deliverysystem_deliveries