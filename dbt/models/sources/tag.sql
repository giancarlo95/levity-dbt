WITH source AS (

    SELECT * FROM {{ source('intercom', 'tag') }}

),

renamed AS (

    SELECT
        id	                     AS tag_id,
        _fivetran_deleted,		
        _fivetran_synced,	
        name                     AS tag_description
    FROM source

)

SELECT *
FROM renamed