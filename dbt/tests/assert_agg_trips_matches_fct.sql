-- Fails if the aggregate loses or adds trips or revenue compared with the fact table.
select a.trips, f.trips as fct_trips, a.total_amount, f.total_amount as fct_total
from (select sum(trips) as trips, round(sum(total_amount), 2) as total_amount from {{ ref('agg_trips_zone_hour') }}) a
cross join (select count(*) as trips, round(sum(total_amount), 2) as total_amount from {{ ref('fct_trips') }}) f
where a.trips <> f.trips or a.total_amount <> f.total_amount