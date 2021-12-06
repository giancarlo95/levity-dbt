WITH source AS (

    SELECT * FROM {{ source('google_cloud_postgresql_public', 'workflows_blendrblock') }}

),

renamed AS (

    SELECT
        id,			
        _fivetran_deleted,	
        _fivetran_synced,			
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
