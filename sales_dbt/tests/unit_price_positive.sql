select *
from {{ model }}
where unit_price < 0
