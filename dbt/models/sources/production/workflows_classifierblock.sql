WITH source AS (

    SELECT * FROM {{ source('public', 'production_workflows_classifierblock') }}

),

renamed AS (

    SELECT
        id                                  AS classifierblock_id,				
        block_id,		
        classifier_id,		
        CAST(created_at AS TIMESTAMP)       AS date_classifierblock_created,                         
        output_status,		
        CAST(owner_id AS STRING)            AS user_id,	
        status,		
        type,		
        CAST(updated_at AS TIMESTAMP)       AS date_classifierblock_updated,
        _airbyte_emitted_at,	
        _airbyte_production_workflows_classifierblock_hashid 		
    
    FROM 
        source

)

SELECT *
FROM renamed
