-- One row per SAP billing item: corporate account invoices (synthetic SAP extract).
select
    concat(billing_doc, '-', item_no) as billing_item_id,
    billing_doc,
    item_no,
    customer_key,
    material_code,
    cast(date_format(billing_date, 'yyyyMMdd') as int) as billing_date_key,
    billed_trips,
    net_amount,
    currency,
    company_code,
    billing_type
from {{ ref('stg_sap_billing_items') }}