SELECT		count(1), 'missing group admin info' as info
from		DDDZ2000YANDEXRU__STAGING.groups as g
left join	DDDZ2000YANDEXRU__STAGING.users as u
			on g.id = u.id
where		u.id is null
	UNION ALL
SELECT		count(1), 'missing sender info' as info
from		DDDZ2000YANDEXRU__STAGING.dialogs as d
left join	DDDZ2000YANDEXRU__STAGING.users as u
			on d.message_from = u.id
where		u.id is null
	UNION ALL
SELECT		count(1), 'missing receiver  info' as info
from		DDDZ2000YANDEXRU__STAGING.dialogs as d
left join	DDDZ2000YANDEXRU__STAGING.users as u
			on d.message_to = u.id
where		u.id is null
	UNION ALL
SELECT		count(1), 'norm receiver info' as info
from		DDDZ2000YANDEXRU__STAGING.dialogs as d
inner join	DDDZ2000YANDEXRU__STAGING.users as u
			on d.message_to = u.id