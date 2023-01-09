drop table if exists public.shipping_transfer CASCADE;
create table public.shipping_transfer (
	id int not null GENERATED ALWAYS AS identity
	,transfer_type text
	,transfer_model text
	,shipping_transfer_rate numeric(14,3)
	,primary key (id)
);	

insert into public.shipping_transfer (transfer_type ,transfer_model ,shipping_transfer_rate)
select	str[1]::text as transfer_type
		,str[2]::text as transfer_model
		,shipping_transfer_rate::numeric (14,3)
from	(select	distinct regexp_split_to_array(shipping_transfer_description , E'\\:+') as str
				,shipping_transfer_rate
		from	public.shipping
		) as shipping_transfer_tbl
;