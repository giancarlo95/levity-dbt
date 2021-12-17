WITH source AS (

    SELECT * FROM {{ source('public', 'workflows_flowstep') }}

),

renamed AS (

    SELECT
        id,			
        owner_id                                                      AS old_user_id,			
        CAST(created_at AS TIMESTAMP)                                 AS date_step_created,			
        updated_at                                                    AS date_step_updated,			
        workflow_id	                                                  AS flow_id,		
        frontegg_user_id                                              AS user_id,			
        frontegg_tenant_id                                            AS account_id
    FROM 
        source

)

SELECT *
FROM renamed
