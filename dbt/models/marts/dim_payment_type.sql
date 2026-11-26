select payment_type_key, payment_desc
from (values
    (0, 'Flex Fare'), (1, 'Credit card'), (2, 'Cash'), (3, 'No charge'),
    (4, 'Dispute'), (5, 'Unknown'), (6, 'Voided trip'), (-1, 'Missing')
) as t(payment_type_key, payment_desc)