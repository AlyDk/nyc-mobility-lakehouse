-- Current vendor version from the SCD2 history in silver (history starts Nov 2026, so trips join to current).
select
    vendor_id as vendor_key,
    vendor_name,
    platform_fee_pct,
    valid_from as current_since
from {{ source('silver', 'dim_vendor_scd2') }}
where is_current