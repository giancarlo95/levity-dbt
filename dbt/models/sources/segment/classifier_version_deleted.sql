WITH source AS (

    SELECT * FROM {{ source('django_backend_api', 'classifier_version_deleted') }}

),

renamed AS (

    SELECT
        _id		                                        AS version_id,	
        event      			
    FROM 
        source

)

SELECT *
FROM renamed