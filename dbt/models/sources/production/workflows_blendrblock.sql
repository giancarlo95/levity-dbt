WITH source AS (

    SELECT * FROM {{ source('public', 'workflows_blendrblock') }}

),

renamed AS (

    SELECT
        id,					
        blendr_blend_id,			
        block_id,			
        created_at,			
        datasource_id                AS blendrdatasource_id,			
        owner_id,		
        status,			
        template_id                  AS blendrtemplate_id,			
        updated_at,			
        frontegg_tenant_id	         AS account_id,		
        frontegg_user_id             AS user_id				
    FROM 
        source
    
)

SELECT *
FROM renamed
