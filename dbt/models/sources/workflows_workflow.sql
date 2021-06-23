WITH source AS (

    SELECT * FROM {{ source('public', 'production_workflows_workflow') }}

),

renamed AS (

    SELECT
        id                                       AS flow_id,						
        created_at                               AS date_flow_created,			
        description                              AS flow_description,		
        name,			
        CAST(owner_id AS STRING)                 AS user_id,	
        status                                   AS flow_status,		
        updated_at,
        _airbyte_emitted_at,	
        _airbyte_production_workflows_workflow_hashid 			
    
    FROM 
        source

)

SELECT *
FROM renamed
