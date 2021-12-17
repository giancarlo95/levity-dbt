WITH source AS (

    SELECT * FROM {{ source('public', 'workflows_classifierblock') }}

),

renamed AS (

    SELECT
        id                                  AS classifierblock_id,				
        block_id,		
        classifier_id,		
        CAST(created_at AS TIMESTAMP)       AS date_classifierblock_created,                         
        output_status,		
        CAST(owner_id AS STRING)            AS old_user_id,
        frontegg_user_id                    AS user_id,
        frontegg_tenant_id                  AS account_id,	
        status,		
        type,		
        CAST(updated_at AS TIMESTAMP)       AS date_classifierblock_updated
    FROM 
        source

)

SELECT *
FROM renamed
