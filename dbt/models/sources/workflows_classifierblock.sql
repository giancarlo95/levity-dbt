WITH source AS (

    SELECT * FROM {{ source('public', 'production_workflows_classifierblock') }}

),

renamed AS (

    SELECT
        id                                  AS classifierblock_id,				
        block_id,		
        classifier_id,		
        created_at,                         
        output_status,		
        CAST(owner_id AS STRING)            AS user_id,	
        status,		
        type,		
        updated_at,
        _airbyte_emitted_at,	
        _airbyte_production_workflows_classifierblock_hashid 		
    
    FROM 
        source

)

SELECT *
FROM renamed
