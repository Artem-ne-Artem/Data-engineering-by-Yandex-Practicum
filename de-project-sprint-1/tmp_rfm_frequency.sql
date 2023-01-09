/*
CREATE TABLE analysis.tmp_rfm_frequency (
 user_id INT NOT NULL PRIMARY KEY,
 frequency INT NOT NULL CHECK(frequency >= 1 AND frequency <= 5)
);
*/

insert into analysis.tmp_rfm_frequency
SELECT		u.id AS user_id ,ntile(5) over (order by count(o.order_id) asc) as frequency  
FROM 		analysis.users as u 
LEFT JOIN 	analysis.orders as o ON o.user_id = u.id AND o.order_ts >= '2021-01-01' AND o.status = 4
;