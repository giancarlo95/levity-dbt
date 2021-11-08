WITH source AS (

    SELECT * FROM {{ source('django_backend_api', 'dataset_deleted_view') }}

),

renamed AS (

    SELECT
        *,
        _id		                                        AS aiblock_id     			
    FROM 
        source

)

SELECT *
FROM renamed