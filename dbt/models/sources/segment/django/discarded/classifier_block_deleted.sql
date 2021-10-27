WITH source AS (

    SELECT * FROM {{ source('django_backend_api', 'classifier_block_deleted') }}

),

renamed AS (

    SELECT
        _id		                                        AS classifierblock_id,	
        event      			
    FROM 
        source

)

SELECT *
FROM renamed