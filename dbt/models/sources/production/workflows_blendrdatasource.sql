WITH source AS (

    SELECT * FROM {{ source('google_cloud_postgresql_public', 'workflows_blendrdatasource') }}

),

renamed AS (

    SELECT
        id                                   AS blendrdatasource_id, 			
        _fivetran_deleted,			
        _fivetran_synced,			
        blendr_datasource_id,			
        created_at,			
        logo_url,			
        name                                 AS blendrdatasource_name,			
        owner_id,			
        updated_at,			
        frontegg_tenant_id	                 AS account_id,		
        frontegg_user_id	                 AS user_id			
    FROM 
        source
    
)

SELECT *
FROM renamed
