-- Valid trips only; invalid rows stay in silver with their dq_flags.
select
    trip_id,
    service,
    vendor_id,
    pickup_ts,
    dropoff_ts,
    pickup_date,
    hour(pickup_ts) as pickup_hour,
    pu_location_id,
    do_location_id,
    passenger_count,
    trip_distance_mi,
    duration_min,
    rate_code_id,
    coalesce(payment_type, -1) as payment_type,
    is_flex_fare,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    congestion_surcharge,
    airport_fee,
    total_amount,
    _period
from {{ source('silver', 'trips') }}
where is_valid