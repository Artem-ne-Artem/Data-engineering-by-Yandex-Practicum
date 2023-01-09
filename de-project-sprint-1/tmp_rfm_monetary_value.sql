/*
CREATE TABLE analysis.tmp_rfm_monetary_value (
 user_id INT NOT NULL PRIMARY KEY,
 monetary_value INT NOT NULL CHECK(monetary_value >= 1 AND monetary_value <= 5)
);
*/

insert into analysis.tmp_rfm_monetary_value
SELECT		u.id AS user_id ,ntile(5) over (order by sum(payment) asc nulls first) AS monetary_value 
FROM 		analysis.users as u 
LEFT JOIN 	analysis.orders as o ON o.user_id = u.id AND o.order_ts >= '2021-01-01' AND o.status = 4
;