WITH workflows_workflow AS (

    SELECT * FROM {{ref('workflows_workflow')}}

)

SELECT
	user_id,
	min(date_flow_created) AS date_first_flow_created
FROM
	workflows_workflow
GROUP BY
	user_id