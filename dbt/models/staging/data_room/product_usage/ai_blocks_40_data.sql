WITH datasets_data_large AS (

    SELECT
		aiblock_id
	FROM
		{{ref('datasets_data_deleted')}}
	GROUP BY
		aiblock_id
	HAVING
		COUNT(datapoint_id)>=40

), datasets_dataset AS (

    SELECT 
        aiblock_id,
        is_template
    FROM 
        {{ref('datasets_dataset_deleted')}}

), datasets_data AS (

    SELECT 
       account_id,
       aiblock_id,
	   date_datapoint_uploaded
    FROM 
       {{ref('datasets_data_deleted')}}
    WHERE 
        account_id IS NOT NULL

), onboarded_accounts AS (

    SELECT 
       *
    FROM 
       {{ref('onboarded_accounts')}}

), final AS (

    SELECT 
        datasets_data.account_id,
        datasets_data.aiblock_id,
        datasets_dataset.is_template,
        CASE 
            WHEN onboarded_accounts.customer_status="Paid Plan" THEN "Paid Plan"
            ELSE "Design Partner"
        END                                         AS customer_status_binary,
        date_datapoint_uploaded
    FROM datasets_data
    INNER JOIN datasets_dataset ON 
        datasets_dataset.aiblock_id=datasets_data.aiblock_id
    INNER JOIN datasets_data_large ON
        datasets_data_large.aiblock_id=datasets_data.aiblock_id
    INNER JOIN onboarded_accounts ON
        onboarded_accounts.account_id=datasets_data.account_id

), final_other AS (

    SELECT 
        account_id,
        aiblock_id,
        is_template,
        customer_status_binary,
        date_datapoint_uploaded,
        ROW_NUMBER() OVER (PARTITION BY account_id, aiblock_id ORDER BY date_datapoint_uploaded) AS RowNumber
    FROM 
        final

)

SELECT 
    *
FROM 
    final_other
WHERE 
    RowNumber=40


