WITH datasets_dataset AS (

    SELECT * FROM {{ref('datasets_dataset_deleted')}}

)

SELECT
	account_id,
	min(date_aiblock_created) AS date_first_aiblock_created
FROM
	datasets_dataset
WHERE
	aiblock_description IS NULL
GROUP BY
	account_id