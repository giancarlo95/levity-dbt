select 
ch.id as contact_id,
ch.email as contact_email,
ch.location_country as contact_country
from {{ source('intercom', 'contact_history') }} ch
inner join (
select
email,
max(updated_at) as date_last_update
from {{ source('intercom', 'contact_history') }} 
where role='user'
group by email) as interm
on interm.email=ch.email and ch.updated_at=interm.date_last_update