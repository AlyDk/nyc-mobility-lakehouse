{{ config(materialized='table', liquid_clustered_by=['pickup_date_key']) }}

select
    trip_id,
    service,
    vendor_id,
    cast(date_format(pickup_date, 'yyyyMMdd') as int) as pickup_date_key,
    pickup_hour,
    pu_location_id as pu_zone_key,
    do_location_id as do_zone_key,
    payment_type as payment_type_key,
    is_flex_fare,
    coalesce(rate_code_id = 2 or pu_location_id in (1, 132, 138) or do_location_id in (1, 132, 138), false) as is_airport_trip,
    passenger_count,
    trip_distance_mi,
    duration_min,
    fare_amount,
    tip_amount,
    tolls_amount,
    coalesce(extra, 0) + coalesce(mta_tax, 0) + coalesce(improvement_surcharge, 0)
      + coalesce(congestion_surcharge, 0) + coalesce(airport_fee, 0) as surcharges_amount,
    total_amount,
    _period
from {{ ref('stg_trips') }}