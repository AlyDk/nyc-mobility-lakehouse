-- Fails for any SAP invoice whose header net value (VBRK) differs from the sum of its items (VBRP).
select k.VBELN, cast(k.NETWR as decimal(15, 2)) as header_net, p.items_net
from {{ source('bronze', 'sap_vbrk') }} k
join (select VBELN, sum(cast(NETWR as decimal(15, 2))) as items_net
      from {{ source('bronze', 'sap_vbrp') }} group by VBELN) p
    on p.VBELN = k.VBELN
where cast(k.NETWR as decimal(15, 2)) <> p.items_net