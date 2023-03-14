select 		count(1)
from		DDDZ2000YANDEXRU__STAGING.groups as g
left join	DDDZ2000YANDEXRU__STAGING.users as u
			on g.id = u.id
where		u.id is null