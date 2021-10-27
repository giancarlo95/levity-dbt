WITH source AS (

    SELECT * FROM {{ source('django_backend_api', 'dataset_created_view') }}

),

renamed AS (

    SELECT
        *,
        _id		                                        AS aiblock_id,	
        event      			
    FROM 
        source

)

SELECT *
FROM renamed