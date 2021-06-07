select 
ch.id as contact_id,
ch.location_country as contact_country
from {{ source('intercom', 'contact_history') }} ch
inner join (
select
id as id,
max(updated_at) as date_last_update
from {{ source('intercom', 'contact_history') }} 
group by id) as interm
on interm.id=ch.id and ch.updated_at=interm.date_last_update