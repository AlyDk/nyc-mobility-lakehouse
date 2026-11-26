-- Fails if the fact table does not hold exactly the valid silver trips.
select f.n as fact_rows, s.n as silver_valid_rows
from (select count(*) as n from {{ ref('fct_trips') }}) f
cross join (select count(*) as n from {{ source('silver', 'trips') }} where is_valid) s
where f.n <> s.n