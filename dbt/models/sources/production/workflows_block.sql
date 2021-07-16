WITH source AS (

    SELECT * FROM {{ source('public', 'production_workflows_block') }}

),

renamed AS (

    SELECT
        id                                              AS block_id,			
        CAST(created_at AS TIMESTAMP)                   AS date_block_created,		
        CAST(owner_id AS STRING)                        AS user_id,	
        parent_id,	
        starting_block                                  AS is_trigger,	
        type                                            AS block_type,		
        CAST(updated_at AS TIMESTAMP)                   AS date_block_updated,		
        workflow_id	                                    AS flow_id,
        _airbyte_emitted_at,	
        _airbyte_production_workflows_block_hashid 		
    
    FROM 
        source

)

SELECT *
FROM renamed
