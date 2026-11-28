-- VBRP billing items joined to VBRK headers. Dates arrive as YYYYMMDD text, amounts as text.
select
    cast(p.VBELN as bigint) as billing_doc,
    cast(p.POSNR as int) as item_no,
    cast(k.KUNAG as int) as customer_key,
    to_date(k.FKDAT, 'yyyyMMdd') as billing_date,
    p.MATNR as material_code,
    cast(p.FKIMG as int) as billed_trips,
    cast(p.NETWR as decimal(15, 2)) as net_amount,
    k.WAERK as currency,
    k.BUKRS as company_code,
    k.FKART as billing_type
from {{ source('bronze', 'sap_vbrp') }} p
join {{ source('bronze', 'sap_vbrk') }} k
    on k.VBELN = p.VBELN and k.MANDT = p.MANDT
where p.MANDT = '100'