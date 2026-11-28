-- KNA1 customer master. SAP keys are zero-padded text; client 100 only.
select
    cast(KUNNR as int) as customer_key,
    KUNNR as sap_customer_number,
    NAME1 as customer_name,
    ORT01 as city,
    LAND1 as country,
    KTOKD as account_group
from {{ source('bronze', 'sap_kna1') }}
where MANDT = '100'