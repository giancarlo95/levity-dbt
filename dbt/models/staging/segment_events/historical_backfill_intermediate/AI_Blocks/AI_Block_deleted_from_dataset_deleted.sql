WITH dataset_deleted AS (

    SELECT 
        *
    FROM 
        {{ref('dataset_deleted')}}

)

SELECT 
    _id		                                              AS aiblock_id,		
    timestamp                                             AS date_aiblock_deleted,					
    frontegg_tenant_id                                    AS account_id,			
    frontegg_user_id                                      AS user_id,						
    owner_id                                              AS old_user_id
FROM 
    dataset_deleted