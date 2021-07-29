WITH source AS (

    SELECT * FROM {{ source('intercom', 'contact_history') }}

)

SELECT 
    id,
    updated_at,
    _fivetran_deleted
FROM 
    source
WHERE
    _fivetran_deleted=true