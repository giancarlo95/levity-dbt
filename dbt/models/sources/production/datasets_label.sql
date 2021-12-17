WITH source AS (

    SELECT * FROM {{ source('public', 'datasets_label') }}

),

renamed AS (

    SELECT
        id	                                      AS label_id,		
        name                                      AS label_name,			
        color,			
        emoji,			
        status,			
        owner_id                                  AS old_user_id,			
        CAST(created_at	AS TIMESTAMP)             AS date_label_created,		
        dataset_id	                              AS aiblock_id,		
        CAST(updated_at AS TIMESTAMP)             AS date_label_updated,		
        instruction,		
        template_id,			
        frontegg_user_id                          AS user_id,			
        frontegg_tenant_id                        AS account_id					
    FROM source

)

SELECT *
FROM renamed