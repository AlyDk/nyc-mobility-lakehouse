-- Latest SQL Server snapshot of TLC rate codes.
select
    cast(rate_code_id as int) as rate_code_key,
    rate_code_desc
from {{ source('bronze', 'sqlserver_rate_code') }}
qualify extracted_at = max(extracted_at) over ()