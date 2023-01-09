insert into analysis.dm_rfm_segments
SELECT		u.id AS user_id 
      		,ntile(5) over (order by max(order_ts) asc nulls first) as recency 
      		,ntile(5) over (order by count(o.order_id) asc) as frequency  
      		,ntile(5) over (order by sum(payment) asc nulls first) AS monetary_value 
FROM 		analysis.users as u 
LEFT JOIN 	analysis.orders as o
			ON o.user_id = u.id   
			AND o.order_ts >= '2021-01-01'  
			AND o.status = 4
GROUP BY 	1
---------------------------------------------------------------------------------------
| user_id | recency | frequency | monetary_value| 
| 0 | 1 | 3 | 4 |
| 1 | 4 | 3 | 3 |
| 2 | 2 | 4 | 5 |
| 3 | 2 | 3 | 3 |
| 4 | 4 | 3 | 3 |
| 5 | 5 | 5 | 5 |
| 6 | 1 | 4 | 5 |
| 7 | 4 | 2 | 2 |
| 8 | 1 | 2 | 3 |
| 9 | 1 | 2 | 2 |
