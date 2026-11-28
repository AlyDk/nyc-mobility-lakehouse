-- Fails if SAP billing does not equal the corporate trips it was billed from (same rule as the extract).
with billed as (
    select sum(billed_trips) as trips, sum(net_amount) as amount from {{ ref('fct_billing_items') }}
),
corporate as (
    select count(*) as trips, sum(f.total_amount) as amount
    from {{ ref('fct_trips') }} f
    join {{ ref('dim_zone') }} z on f.pu_zone_key = z.zone_key
    where z.borough = 'Manhattan'
      and f.payment_type_key in (0, 1)
      and abs(xxhash64(f.trip_id)) % 20 = 0
)
select b.trips as billed_trips, c.trips as corporate_trips, b.amount as billed_amount, c.amount as corporate_amount
from billed b cross join corporate c
where b.trips <> c.trips or b.amount <> c.amount