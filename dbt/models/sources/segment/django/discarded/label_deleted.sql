WITH source AS (

    SELECT * FROM {{ source('django_backend_api', 'label_deleted') }}

),

renamed AS (

    SELECT
        _id		                                        AS label_id,	
        event      			
    FROM 
        source

)

SELECT *
FROM renamed