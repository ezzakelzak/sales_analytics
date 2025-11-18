select *
from {{ model }}
where event_timestamp > current_timestamp()