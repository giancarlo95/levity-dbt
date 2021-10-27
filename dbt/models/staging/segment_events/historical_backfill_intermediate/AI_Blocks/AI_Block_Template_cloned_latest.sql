WITH AI_Block_Template_cloned AS (

    SELECT 
        *
    FROM 
        {{ref('AI_Block_Template_cloned')}}

)

SELECT 	
    timestamp               AS date_aiblock_created,
    user_id,
    dataset_id              AS aiblock_id,
    email                   AS user_email_address	    	
FROM 
    AI_Block_Template_cloned