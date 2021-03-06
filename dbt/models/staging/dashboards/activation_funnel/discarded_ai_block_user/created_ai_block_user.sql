WITH datasets_dataset AS (

    SELECT * FROM {{ref('datasets_dataset')}}

)

SELECT
	user_id,
	min(date_aiblock_created) AS date_first_aiblock_created
FROM
	datasets_dataset
WHERE
	is_template="no"
GROUP BY
	user_id