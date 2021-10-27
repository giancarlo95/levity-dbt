WITH source AS (

    SELECT * FROM {{ source('django_backend_api', 'workflow_deleted') }}

),

renamed AS (

    SELECT
        _id		                                        AS flow_id,	
        event      			
    FROM 
        source

)

SELECT *
FROM renamed