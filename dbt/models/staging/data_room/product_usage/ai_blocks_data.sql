WITH datasets_dataset AS (

    SELECT 
       account_id,
       aiblock_id,
       is_template
    FROM 
       {{ref('datasets_dataset_deleted')}}

), datasets_data AS (

    SELECT 
       *
    FROM 
       {{ref('datasets_data_deleted')}}
    WHERE 
        account_id IS NOT NULL

), onboarded_accounts AS (

    SELECT 
       *
    FROM 
       {{ref('onboarded_accounts')}}

)

SELECT 
    datasets_data.account_id,
    datasets_data.aiblock_id,
    datasets_dataset.is_template,
    CASE 
        WHEN onboarded_accounts.customer_status="Paid Plan" THEN "Paid Plan"
        ELSE "Design Partner"
    END                          AS customer_status_binary,
	MIN(date_datapoint_uploaded) AS date_somedata_uploaded
FROM 
    datasets_data
INNER JOIN datasets_dataset ON datasets_dataset.aiblock_id=datasets_data.aiblock_id
INNER JOIN onboarded_accounts ON onboarded_accounts.account_id=datasets_dataset.account_id
GROUP BY
    datasets_data.account_id,
    datasets_data.aiblock_id,
    datasets_dataset.is_template,
    CASE 
        WHEN onboarded_accounts.customer_status="Paid Plan" THEN "Paid Plan"
        ELSE "Design Partner"
    END 
