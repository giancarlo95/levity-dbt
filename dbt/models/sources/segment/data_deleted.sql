WITH source AS (

    SELECT * FROM {{ source('django_backend_api', 'data_deleted') }}

),

renamed AS (

    SELECT
        _id		                                        AS datapoint_id,	
        event      			
    FROM 
        source

)

SELECT *
FROM renamed