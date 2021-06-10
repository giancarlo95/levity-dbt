WITH source AS (

    SELECT * FROM {{ source('google_cloud_postgresql_public', 'datasets_dataset') }}

),

renamed AS (

    SELECT
       id                   AS aiblock_id,   	
       _fivetran_deleted,	
       _fivetran_synced,		
       created_at           AS date_aiblock_created,	
       description          AS aiblock_description,		
       emoji,		
       labelers_per_item,		
       multi_label,	
       name,	
       owner_id	            AS user_id,	
       project_id,		
       status,		
       template,		
       type	                AS aiblock_data_type,	
       updated_at   
    FROM source

)

SELECT *
FROM renamed