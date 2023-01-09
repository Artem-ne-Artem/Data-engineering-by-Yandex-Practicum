--Всего строк 324 770

--id является уникальным значением
select count(1) as cnt_all ,count(distinct id) as id from public.shipping

--NULL нет
select 		count(1)
from 		public.shipping
where		id is null or shippingid is null or saleid is null or orderid is nul or clientid is null or payment_amount is null
			or state_datetime is null or productid is null or description is null or vendorid is null or namecategory is null
			or base_country is null or status is null or state is null or shipping_plan_datetime is null or hours_to_plan_shipping is null
			or shipping_transfer_description is null or shipping_transfer_rate is null or shipping_country is null
			or shipping_country_base_rate is null or vendor_agreement_description is null

--saleid ,есть дубли ,не уник строк - 54 143
select count(1) as cnt_all ,count(distinct saleid) as uniq_saleid from public.shipping

--orderid ,есть дубли ,не уник строк - 54 143
select count(1) as cnt_all ,count(distinct orderid) as uniq_orderid from public.shipping

--clientid ,есть дубли ,не уник строк - 49 962
select count(1) as cnt_all ,count(distinct clientid) as uniq_clientid from public.shipping

--payment_amount отрицательных значений нет
select min(payment_amount) as "min", max(payment_amount) as "max" ,avg(payment_amount) as "avg" from public.shipping

--мин дата 2021-08-09 макс дата 2022-07-05, дат формата 1900 нет
select min(state_datetime::date), max(state_datetime::date) from public.shipping

--мин время 00:00:00 макс время 23:59:59, выбросов нет
select min(state_datetime::time), max(state_datetime::time) from public.shipping

--в поле state_datetime есть разрывы хронологии, отсутствуют даты 2022-06-24 2022-06-27 2022-07-01 2022-07-02
with tbl_1 as (select distinct state_datetime::date as state_date from public.shipping)
,tbl_2 as (
select		distinct  state_date ,lead(state_date) over (order by state_date asc) as lead_dt
			,lead(state_date) over (order by state_date asc) - state_date as delta
from 		tbl_1
order by	state_date asc
)
select * from tbl_2  where delta != 1 or delta is null

--мин дата 2021-08-10 макс дата 2022-07-02, выбросов нет
select min(shipping_plan_datetime::date), max(shipping_plan_datetime::date) from public.shipping

--мин время 00:00:00 макс время 23:59:59, выбросов нет
select min(state_datetime::time), max(state_datetime::time) from public.shipping

--есть отрицательные значения, значит товар доставлен раньше планового времени
select min(hours_to_plan_shipping), max(hours_to_plan_shipping) from public.shipping

--отрицательных значений нет
select min(shipping_transfer_rate), max(shipping_transfer_rate) from public.shipping

--отрицательных значений нет
select min(shipping_country_base_rate), max(shipping_country_base_rate) from public.shipping