select *
from {{ model }}
where length(country_code) != 2
   or country_code not in ('US','FR', 'ES', 'IT', 'UK', 'JP')