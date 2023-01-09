/*
CREATE TABLE analysis.tmp_rfm_recency (
 user_id INT NOT NULL PRIMARY KEY,
 recency INT NOT NULL CHECK(recency >= 1 AND recency <= 5)
);
*/

insert into analysis.tmp_rfm_recency
SELECT		u.id AS user_id ,ntile(5) over (order by max(order_ts) asc nulls first) as recency
FROM 		analysis.users as u 
LEFT JOIN 	analysis.orders as o ON o.user_id = u.id AND o.order_ts >= '2021-01-01' AND o.status = 4
;