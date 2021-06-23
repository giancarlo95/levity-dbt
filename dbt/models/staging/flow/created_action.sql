WITH workflows_block AS (

    SELECT * FROM {{ref('workflows_block')}}

)

SELECT
	user_id,
	min(date_block_created) AS date_first_action_created
FROM
	workflows_block
WHERE 
    is_trigger=FALSE 
    AND block_type='blendr'
GROUP BY
	user_id
