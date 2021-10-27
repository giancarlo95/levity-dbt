WITH source AS (

    SELECT * FROM {{ source('django_backend_api', 'classifier_deleted') }}

),

renamed AS (

    SELECT
        _id		                                        AS classifier_id,	
        event      			
    FROM 
        source

)

SELECT *
FROM renamed