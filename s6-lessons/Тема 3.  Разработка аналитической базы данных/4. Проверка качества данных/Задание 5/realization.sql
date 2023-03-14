SELECT 		max(registration_dt) < now() as 'no future dates',
		    min(registration_dt) > '2020-09-03' as 'no false-start dates',
			'users' as dataset
FROM		DDDZ2000YANDEXRU__STAGING.users
	UNION ALL
SELECT 		max(registration_dt) < now() as 'no future dates',
		    min(registration_dt) > '2020-09-03' as 'no false-start dates',
			'groups' as dataset
FROM		DDDZ2000YANDEXRU__STAGING.groups
	UNION ALL
SELECT 		max(message_ts) < now() as 'no future dates',
		    min(message_ts) > '2020-09-03' as 'no false-start dates',
			'dialogs' as dataset
FROM		DDDZ2000YANDEXRU__STAGING.dialogs