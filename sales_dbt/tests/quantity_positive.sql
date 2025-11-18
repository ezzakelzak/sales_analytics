select *
from {{ model }}
where quantity <= 0