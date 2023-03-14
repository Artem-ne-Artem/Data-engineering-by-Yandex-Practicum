with message as (
select		hk_message_id
from		DDDZ2000YANDEXRU__DWH.l_groups_dialogs
where 		hk_group_id in (select hk_group_id from DDDZ2000YANDEXRU__DWH.h_groups order by registration_dt limit 10)
)

, user_id as (
select		um.hk_user_id
from		DDDZ2000YANDEXRU__DWH.l_user_message as um
inner join	message m
			on um.hk_message_id = m.hk_message_id
)

select		s.age ,count(distinct u.hk_user_id) as count
from		user_id as u
left join	DDDZ2000YANDEXRU__DWH.s_user_socdem as s
			on u.hk_user_id = s.hk_user_id
group by	1
order by    2