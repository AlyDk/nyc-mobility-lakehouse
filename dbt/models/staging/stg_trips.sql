-- Valid trips only; invalid rows stay in silver with their dq_flags.
-- Money is cast to DECIMAL(10,2) so sums are exact (double caused cent-level drift).
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
    cast(fare_amount as decimal(10, 2)) as fare_amount,
    cast(extra as decimal(10, 2)) as extra,
    cast(mta_tax as decimal(10, 2)) as mta_tax,
    cast(tip_amount as decimal(10, 2)) as tip_amount,
    cast(tolls_amount as decimal(10, 2)) as tolls_amount,
    cast(improvement_surcharge as decimal(10, 2)) as improvement_surcharge,
    cast(congestion_surcharge as decimal(10, 2)) as congestion_surcharge,
    cast(airport_fee as decimal(10, 2)) as airport_fee,
    cast(total_amount as decimal(10, 2)) as total_amount,
    _period
from {{ source('silver', 'trips') }}
where is_valid