{{ config(materialized='table', sort='contact_country', dist='user_email_address') }}
select 
user_email_address,
contact_country
from {{ref('production_users')}} pu
inner join {{ref('intercom_contacts')}} ic
on pu.user_email_address=ic.contact_email