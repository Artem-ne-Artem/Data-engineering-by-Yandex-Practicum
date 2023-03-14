SELECT min(u.registration_dt) as datestamp ,'earliest user registration' as info FROM DDDZ2000YANDEXRU__STAGING.users u
	UNION ALL
SELECT max(u.registration_dt) as datestamp ,'latest user registration' FROM DDDZ2000YANDEXRU__STAGING.users u
	UNION ALL
SELECT min(registration_dt) as datestamp ,'earliest group creation' as info FROM DDDZ2000YANDEXRU__STAGING.groups
	UNION ALL
SELECT min(registration_dt) as datestamp ,'latest group creation' as info FROM DDDZ2000YANDEXRU__STAGING.groups
	UNION ALL
SELECT min(message_ts) as datestamp ,'earliest dialog message' as info FROM DDDZ2000YANDEXRU__STAGING.dialogs
	UNION ALL
SELECT min(message_ts) as datestamp ,'latest dialog message' as info FROM DDDZ2000YANDEXRU__STAGING.dialogs
;