--Значение в поле status должно соответствовать последнему по времени статусу из таблицы production.OrderStatusLog
create view de.analysis.orders as
select 		distinct o.order_id
    		,o.order_ts
    		,o.user_id
    		,o.bonus_payment
    		,o.payment
    		,o."cost"
    		,o.bonus_grant
			,LAST_VALUE(p.status_id) over (partition by p.order_id order by dttm rows between current row and unbounded following) as status
from		production.orders AS o
inner join	production.orderstatuslog AS p
			on o.order_id  = p.order_id
;
