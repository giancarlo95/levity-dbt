WITH source AS (

    SELECT * FROM {{ source('google_cloud_postgresql_public', 'workflows_blendrtemplate_datasources') }}

),

renamed AS (

    SELECT
        id,		
        _fivetran_deleted,		
        _fivetran_synced,		
        blendrdatasource_id,		
        blendrtemplate_id
    FROM 
        source
    
)

SELECT *
FROM renamed
