--Напишите DDL-запрос для создания витрины
CREATE TABLE de.analysis.dm_rfm_segments (
user_id INT NOT NULL PRIMARY KEY
,recency INT NOT NULL
,frequency INT NOT NULL
,monetary_value INT NOT NULL
)
