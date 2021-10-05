WITH pages_view AS (
    
    SELECT 
        DISTINCT anonymous_id 
    FROM 
        {{ref("pages_view")}} 
    WHERE 
        user_id IS NULL
    
), sign_up_view AS (
    
    SELECT 
        DISTINCT anonymous_id 
    FROM 
        {{ref("sign_up_view")}} 
    
)

SELECT
*
FROM 
