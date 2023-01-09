drop table if exists public.shipping_info CASCADE;
create table public.shipping_info (
	shippingid bigint
	,vendorid bigint
	,payment_amount numeric(14, 2)
	,shipping_plan_datetime timestamp
	,transfer_type_id bigint
	,shipping_country_id bigint
	,agreementid bigint
	,primary key (shippingid)
);	

--создаём связь с справочником public.shipping_transfer
alter table public.shipping_info add constraint shipping_info_transfer_type_id_fkey
	foreign key (transfer_type_id) references public.shipping_transfer (id)
		ON UPDATE CASCADE;	
--создаём связь с справочником public.shipping_country_rates
alter table public.shipping_info add constraint shipping_info_shipping_country_id_fkey
	foreign key (shipping_country_id) references public.shipping_country_rates (id)
		ON UPDATE CASCADE;			
--создаём связь с справочником public.shipping_agreement
alter table public.shipping_info add constraint shipping_info_shipping_agreementid_fkey
	foreign key (agreementid) references public.shipping_agreement (agreementid)
		ON UPDATE CASCADE;

insert into public.shipping_info
select		distinct t1.shippingid ,t1.vendorid, t1.payment_amount, t1.shipping_plan_datetime
			,st.id as transfer_type_id
			,scr.id as shipping_country_id
			,(regexp_split_to_array(t1.vendor_agreement_description , E'\\:+'))[1]::bigint as agreementid
from		public.shipping as t1
left join	public.shipping_transfer as st
			on t1.shipping_transfer_description = concat(st.transfer_type, ':', st.transfer_model)
left join	public.shipping_country_rates as scr on t1.shipping_country = scr.shipping_country
;	