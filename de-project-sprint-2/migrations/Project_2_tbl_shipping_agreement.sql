drop table if exists public.shipping_agreement CASCADE;
create table public.shipping_agreement (
	agreementid bigint
	,agreement_number text
	,agreement_rate numeric(14,2)
	,agreement_commission numeric(14,2)
	,primary key (agreementid)
);

INSERT INTO public.shipping_agreement (agreementid, agreement_number, agreement_rate, agreement_commission)
select		distinct vendor_agreement_descr[1]::bigint as agreementid
			,vendor_agreement_descr[2]::text as agreement_number
			,vendor_agreement_descr[3]::numeric(14,2) as agreement_rate
			,vendor_agreement_descr[4]::numeric(14,2) as agreement_commission
from		(select	distinct regexp_split_to_array(vendor_agreement_description , E'\\:+') as vendor_agreement_descr
			from 	public.shipping
			) as vendor_tbl
;