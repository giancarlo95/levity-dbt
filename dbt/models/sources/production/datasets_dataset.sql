WITH source AS (

    SELECT * FROM {{ source('google_cloud_postgresql_public', 'datasets_dataset') }}

),

renamed AS (

    SELECT
       id                                   AS aiblock_id,   			
       CAST(created_at AS TIMESTAMP)        AS date_aiblock_created,	
       description                          AS aiblock_description,	
       CASE 
            WHEN description IS NULL then "no"	
            ELSE "yes"
       END                                  AS is_template,
       emoji,		
       labelers_per_item,		
       multi_label,	
       name,	
       CAST(owner_id AS STRING)             AS old_user_id,
       frontegg_user_id                     AS user_id,
       frontegg_tenant_id                   AS account_id,	
       project_id,		
       status,		
       template,		
       type	                                AS aiblock_data_type,	
       CAST(updated_at AS TIMESTAMP)        AS date_aiblock_updated,
       --_airbyte_emitted_at,	
        --_airbyte_production_accounts_paymentplan_hashid
       _fivetran_deleted,
       _fivetran_synced  
    FROM source

)

SELECT *
FROM renamed