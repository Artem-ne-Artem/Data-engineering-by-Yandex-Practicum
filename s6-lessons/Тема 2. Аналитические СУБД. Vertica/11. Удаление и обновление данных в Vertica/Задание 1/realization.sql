SET SESSION AUTOCOMMIT TO off;

-- Напишите запрос, чтобы удалить из таблицы members записи о пользователях старше 45 лет.
DELETE FROM DDDZ2000YANDEXRU.members WHERE age > 45;

-- Напишите запрос к системной таблице DELETE_VECTOR, чтобы увидеть список удалённых строк.
SELECT node_name, projection_name, deleted_row_count FROM DELETE_VECTORS WHERE PROJECTION_NAME LIKE 'members%';

-- Напишите запрос к системной таблице DELETE_VECTOR, чтобы найти узел кластера с самым большим количеством DV-файлов.
-- В наборе данных на выходе должна быть одна колонка.
SELECT MAX(deleted_row_count) FROM DELETE_VECTORS WHERE PROJECTION_NAME LIKE 'members%';

-- Восстановите удалённые строки командой
ROLLBACK;