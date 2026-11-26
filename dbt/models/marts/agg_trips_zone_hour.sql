-- Power BI import table: one row per day, hour, service, pickup zone, payment type, airport and flex flag.
select
    pickup_date_key,
    pickup_hour,
    service,
    pu_zone_key,
    payment_type_key,
    is_airport_trip,
    is_flex_fare,
    count(*) as trips,
    sum(passenger_count) as passengers,
    sum(trip_distance_mi) as distance_mi,
    sum(duration_min) as duration_min,
    sum(fare_amount) as fare_amount,
    sum(tip_amount) as tip_amount,
    sum(total_amount) as total_amount
from {{ ref('fct_trips') }}
group by all