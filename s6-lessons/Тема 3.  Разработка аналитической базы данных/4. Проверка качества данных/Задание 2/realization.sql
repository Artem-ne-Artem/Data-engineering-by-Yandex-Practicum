select count(id) as total,count(distinct id) as uniq, 'users' as dataset from DDDZ2000YANDEXRU__STAGING.users
	union all
select count(id) as total,count(distinct id) as uniq, 'groups' as dataset from DDDZ2000YANDEXRU__STAGING.groups
	union all
select count(message_id) as total,count(distinct message_id) as uniq, 'dialogs' as dataset from DDDZ2000YANDEXRU__STAGING.dialogs