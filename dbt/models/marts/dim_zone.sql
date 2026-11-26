select
    location_id as zone_key,
    borough,
    zone,
    service_zone,
    location_id in (1, 132, 138) as is_airport
from {{ ref('stg_zones') }}