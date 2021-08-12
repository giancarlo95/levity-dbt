WITH source AS (

    SELECT * FROM {{ source('django_backend_api', 'block_deleted') }}

),

renamed AS (

    SELECT
        _id		                                        AS block_id,	
        event      			
    FROM 
        source

)

SELECT *
FROM renamed