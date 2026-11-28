-- MARA material master with English descriptions from MAKT.
select
    m.MATNR as material_code,
    t.MAKTX as material_desc,
    m.MTART as material_type,
    m.MATKL as material_group,
    m.MEINS as base_unit
from {{ source('bronze', 'sap_mara') }} m
left join {{ source('bronze', 'sap_makt') }} t
    on t.MATNR = m.MATNR and t.MANDT = m.MANDT and t.SPRAS = 'E'
where m.MANDT = '100'