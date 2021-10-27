WITH AI_Block_created AS (

    SELECT 
        *
    FROM 
        {{ref('AI_Block_created')}}

)

SELECT 	
    timestamp               AS date_aiblock_created,
    user_id,
    dataset_id              AS aiblock_id,
    email                   AS user_email_address	    	
FROM 
    AI_Block_created