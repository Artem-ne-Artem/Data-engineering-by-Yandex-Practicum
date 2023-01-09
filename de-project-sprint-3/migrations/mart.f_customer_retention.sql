with customers as (
select		s.item_id , s.customer_id ,c.week_of_year as period_id
			,case when count(1) over(partition by s.date_id, s.customer_id) = 1 then 1 else 0 end as flag_new
			,case when count(1) over(partition by s.date_id, s.customer_id) > 1 and payment_amount > 0 then 1 else 0 end as flag_returning
			,case when payment_amount < 0 then 1 else 0 end as flag_refunded
			,s.quantity  ,s.payment_amount 
from		mart.f_sales as s
join 		mart.d_calendar as c
			on s.date_id = c.date_id
where 		1=1
			and  week_of_year = DATE_PART('week', '{{ds}}'::DATE)
--			and c.week_of_year = 50
--			and s.customer_id in (5010 ,6507 ,2009)
)

,new_customers as (
select		period_id, item_id
			,count(distinct customer_id)as new_customers_count
			,sum(payment_amount) as new_customers_revenue
from		customers
where		flag_new = 1
group by	1,2
)

,returning_customers as (
select		period_id, item_id
			,count(distinct customer_id)as returning_customers_count
			,sum(payment_amount) as returning_customers_revenue
from		customers
where		flag_returning = 1
group by	1,2
)

,refunded_customers as (
select		period_id, item_id
			,count(distinct customer_id) as refunded_customer_count 
			,sum(quantity) as customers_refunded
from		customers
where		flag_refunded = 1
group by	1,2
)

select		'weekly' as period_name ,c.period_id, c.item_id
			,sum(n.new_customers_count) as new_customers_count
			,sum(rt.returning_customers_count) as returning_customers_count
			,sum(rf.refunded_customer_count) as refunded_customer_count
			
			,sum(n.new_customers_revenue) as new_customers_revenue
			,sum(rt.returning_customers_revenue) as returning_customers_revenue
			,sum(rf.customers_refunded) as customers_refunded
from		customers as c
left join	new_customers as n on c.item_id = n.item_id and c.period_id = n.period_id
left join	returning_customers as rt on c.item_id = rt.item_id and c.period_id = rt.period_id
left join	refunded_customers as rf on c.item_id = rf.item_id and c.period_id = rf.period_id
group by	1,2,3