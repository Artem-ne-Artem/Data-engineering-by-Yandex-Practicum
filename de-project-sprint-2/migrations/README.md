**Цель** - оптимизировать нагрузку на хранилище.

**Задача** - провести миграцию данных из лога shipping в новую витрину.

**Решение** - построить таблицы справочники (shipping_country_rates, shipping_transfer, shipping_agreement), построить таблицы с фактами (shipping_status, shipping_info). На базе таблиц справочников и таблиц фактов построить витрину.

**Очерёдность запуска скриптов**
1. создаём таблицы справочники (shipping_country_rates, shipping_transfer, shipping_agreement)
2. создаём таблицы с фактами (shipping_status, shipping_info)
3. создаём витрину (shipping_datamart)

**Описание сущностей и атрибутов**
#### shipping_country_rates - справочник стоимости доставки в страны
	id - уникальный идентификатор строки
	shipping_country — страна доставки
	shipping_country_base_rate — налог на доставку в страну, который является процентом от стоимости payment_amount
#### shipping_transfer - справочник о типах доставки
	id - уникальный идентификатор строки
	transfer_type - тип доставки (1p - компания берёт ответственность за доставку на себя, 3p - за отправку ответственен вендор)
	transfer_model - способ доставки (car — машина, train — поезд, ship — корабль, airplane — самолет, multiple — комбинированная доставкоа)
	shipping_transfer_rate - процент стоимости доставки для вендора в зависимости от типа и модели доставки, который взимается интернет-магазином для покрытия расходов
#### shipping_agreement - справочник тарифов доставки вендора
	agreementid - уникальный идентификатор строки
	agreement_number - номер договора
	agreement_rate - тавка налога за стоимость доставки товара для вендора
	agreement_commission - комиссия, то есть доля в платеже являющаяся доходом компании от сделки

#### shipping_info - справочник комиссий по странам с уникальными доставками shippingid
	shippingid - уникальный идентификатор доставки
	vendorid - уникальный идентификатор вендора
	payment_amount - сумма платежа пользователя
	shipping_plan_datetime - плановая дата доставки
	transfer_type_id - внешний ключ для справочника shipping_transfer
	shipping_country_id - внешний ключ для справочника shipping_country_rates
	agreementid - внешний ключ для справочника shipping_agreement

#### shipping_status - таблица статусов о доставке
	shippingid - уникальный идентификатор доставки
	status - статус доставки по конкретному shippingid (in_progress — доставка в процессе; finished — доставка завершена)
	state - статус заказа
		booked - заказано;
		fulfillment — заказ доставлен на склад отправки;
		queued — заказ в очереди на запуск доставки;
		transition — запущена доставка заказа;
		pending — заказ доставлен в пункт выдачи и ожидает получения;
		recieved — покупатель забрал заказ;
		returned — покупатель возвратил заказ после того, как его забрал.
	shipping_start_fact_datetime - время когда state заказа перешёл в состояние booked
	shipping_end_fact_datetime - время когда state заказа перешёл в состояние recieved

#### shipping_datamart - представление (view) на основании справочников и таблиц фактов
	shippingid - уникальный идентификатор доставки
	vendorid - уникальный идентификатор вендора
	transfer_type — тип доставки из таблицы shipping_transfer
	full_day_at_shipping — количество полных дней, в течение которых длилась доставка
	is_delay — статус, показывающий просрочена ли доставка (1 - просрочена; 0 - не просрочена)
	is_shipping_finish — статус, показывающий, что доставка завершена (1 - завершена; 0 - не завершена)
	delay_day_at_shipping — количество дней, на которые была просрочена доставка
	vat — итоговый налог на доставку (payment_amount *(shipping_country_base_rate + agreement_rate + shipping_transfer_rate))
	profit — итоговый доход за доставку (payment_amount * agreement_commission)
