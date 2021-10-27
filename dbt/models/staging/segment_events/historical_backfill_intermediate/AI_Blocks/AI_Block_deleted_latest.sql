WITH AI_Block_deleted AS (

    SELECT 
        *
    FROM 
        {{ref('AI_Block_deleted')}}

)

SELECT 	
    timestamp               AS date_aiblock_deleted,
    user_id,
    dataset_id              AS aiblock_id,
    email                   AS user_email_address	    	
FROM 
    AI_Block_deleted