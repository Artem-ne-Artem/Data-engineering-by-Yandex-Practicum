drop table if exists public.shipping_status CASCADE;
create table public.shipping_status (
	shippingid bigint
	,status text
	,state text
	,shipping_start_fact_datetime timestamp
	,shipping_end_fact_datetime timestamp
	,primary key (shippingid)
);	

insert into public.shipping_status
with shippingid_dt as (
select		shippingid ,min(state_datetime) as min_dt ,max(state_datetime) as max_dt
from		public.shipping
group by 	shippingid
)

select		t1.shippingid
			,t2.status ,t2.state
			,min_dt as shipping_start_fact_datetime
			,case when t2.state in ('recieved', 'returned') then t2.state_datetime end as shipping_end_fact_datetime
from		shippingid_dt as t1
left join	public.shipping as t2
			on t1.shippingid = t2.shippingid
			and t1.max_dt = t2.state_datetime
;