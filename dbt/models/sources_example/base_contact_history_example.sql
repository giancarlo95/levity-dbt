select id, updated_at from {{ source('intercom', 'contact_history') }}
