drop table if exists public.shipping_country_rates CASCADE;
create table public.shipping_country_rates (
	id bigint
	,shipping_country text UNIQUE
	,shipping_country_base_rate numeric(14,3)
	,primary key (id)
);

CREATE SEQUENCE shipping_country_id_sequence START 1;
INSERT INTO public.shipping_country_rates (id, shipping_country, shipping_country_base_rate)
select		nextval('shipping_country_id_sequence') as id
			,shipping_country
			,shipping_country_base_rate
from 		(select distinct shipping_country , shipping_country_base_rate from public.shipping) as tbl_1
;
DROP sequence if exists shipping_country_id_sequence;