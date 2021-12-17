WITH source AS (

    SELECT * FROM {{ source('public', 'workflows_blendrtemplate') }}

),

renamed AS (

    SELECT
        id                         AS blendrtemplate_id,		
        --_fivetran_deleted,			
        --_fivetran_synced,			
        blendr_template_id,			
        created_at,			
        description                AS blendrtemplate_description,			
        name                       AS blendrtemplate_name,			
        owner_id,			
        type                       AS blendrtemplate_type,			
        updated_at,			
        frontegg_tenant_id         AS account_id,			
        frontegg_user_id           AS user_id		
    FROM 
        source
    
)

SELECT *
FROM renamed
