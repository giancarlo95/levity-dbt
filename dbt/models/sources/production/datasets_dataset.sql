WITH source AS (

    SELECT * FROM {{ source('public', 'datasets_dataset') }}

),

renamed AS (

    SELECT
        id                                   AS aiblock_id,   			
        CAST(created_at AS TIMESTAMP)        AS date_aiblock_created,	
        COALESCE(description, "missing")     AS aiblock_description,	
        CASE 
             WHEN description IS NULL then "no"	
             ELSE "yes"
        END                                  AS is_template,
        emoji,		
        labelers_per_item,		
        multi_label,	
        name                                 AS aiblock_name,	
        CAST(owner_id AS STRING)             AS old_user_id,
        frontegg_user_id                     AS user_id,
        frontegg_tenant_id                   AS workspace_id,	
        project_id,		
        status,		
        template,		
        type	                             AS aiblock_data_type,	
        CAST(updated_at AS TIMESTAMP)        AS date_aiblock_updated
    FROM source

)

SELECT *
FROM renamed