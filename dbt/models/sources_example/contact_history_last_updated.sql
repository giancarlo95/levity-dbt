select
id as contact_id,
max(updated_at) as date_contact_last_updated
from {{ source('intercom', 'contact_history') }}
group by id