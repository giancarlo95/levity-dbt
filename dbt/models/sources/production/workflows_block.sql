WITH source AS (

    SELECT * FROM {{ source('google_cloud_postgresql_public', 'workflows_block') }}

),

renamed AS (

    SELECT
        id                                              AS block_id,			
        CAST(created_at AS TIMESTAMP)                   AS date_block_created,		
        CAST(owner_id AS STRING)                        AS old_user_id,
        frontegg_user_id                                AS user_id,
        frontegg_tenant_id                              AS account_id,	
        parent_id,	
        starting_block                                  AS is_trigger,	
        type                                            AS block_type,		
        CAST(updated_at AS TIMESTAMP)                   AS date_block_updated,		
        workflow_id	                                    AS flow_id,
        --_airbyte_emitted_at,	
        --_airbyte_production_accounts_paymentplan_hashid
        _fivetran_deleted,
        _fivetran_synced 		
    
    FROM 
        source

)

SELECT *
FROM renamed
