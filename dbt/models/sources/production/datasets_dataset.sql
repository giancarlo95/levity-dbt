WITH source AS (

    SELECT * FROM {{ source('public', 'production_datasets_dataset') }}

),

renamed AS (

    SELECT
       id                                   AS aiblock_id,   			
       CAST(created_at AS TIMESTAMP)        AS date_aiblock_created,	
       description                          AS aiblock_description,		
       emoji,		
       labelers_per_item,		
       multi_label,	
       name,	
       CAST(owner_id AS STRING)	            AS user_id,	
       project_id,		
       status,		
       template,		
       type	                                AS aiblock_data_type,	
       CAST(updated_at AS TIMESTAMP)        AS date_aiblock_updated,
       _airbyte_emitted_at,	
        _airbyte_production_datasets_dataset_hashid   
    FROM source

)

SELECT *
FROM renamed