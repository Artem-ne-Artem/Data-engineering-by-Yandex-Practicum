select		distinct ((event_value::json->>'product_payments')::json->>1)::json->>'product_name' as product_name
from		outbox
where		((event_value::json->>'product_payments')::json->>1)::json->>'product_name' is not null