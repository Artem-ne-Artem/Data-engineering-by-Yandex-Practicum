SELECT count(hash(g.group_name)) as total, count(distinct hash(g.group_name)) as uniq
FROM DDDZ2000YANDEXRU__STAGING.groups as g