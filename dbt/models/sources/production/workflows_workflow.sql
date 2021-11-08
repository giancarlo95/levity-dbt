WITH source AS (

    SELECT * FROM {{ source('google_cloud_postgresql_public', 'workflows_workflow') }}

),

renamed AS (

    SELECT
        id                                       AS flow_id,						
        CAST(created_at AS TIMESTAMP)            AS date_flow_created,			
        description                              AS flow_description,		
        name,			
        CAST(owner_id AS STRING)                 AS old_user_id,
        frontegg_user_id                         AS user_id,
        frontegg_tenant_id                       AS account_id,	
        status                                   AS flow_status,		
        CAST(updated_at AS TIMESTAMP)            AS date_flow_updated,
        --_airbyte_emitted_at,	
        --_airbyte_production_accounts_paymentplan_hashid
        _fivetran_deleted,
        _fivetran_synced 			
    
    FROM 
        source

)

SELECT *
FROM renamed
