WITH dataset_created AS (

    SELECT 
        *
    FROM 
        {{ref('dataset_created')}}

)

SELECT 
    _id		                                              AS aiblock_id,		
    created_at                                            AS date_aiblock_created,					
    frontegg_tenant_id                                    AS account_id,			
    frontegg_user_id                                      AS user_id,						
    owner_id                                              AS old_user_id
FROM 
    dataset_created
WHERE 
    description IS NOT NULL