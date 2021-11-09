WITH datasets_data AS (

    SELECT
       * 
    FROM 
       {{ref('datasets_data')}}

), data_deleted AS (

    SELECT 
       *
    FROM 
       {{ref('data_deleted')}}

), datasets_dataset_deleted AS (

    SELECT 
        aiblock_id
    FROM 
       {{ref('datasets_dataset_deleted')}}

), final AS(

    SELECT 
    datapoint_id,
    date_datapoint_uploaded,
    datasets_data.aiblock_id, 
    original_file_name,  
    old_user_id,
    user_id,
    account_id,
    remote_url,  
    storage_id,  
    text, 
    date_datapoint_updated
    FROM 
        datasets_data
    INNER JOIN 
        datasets_dataset_deleted ON datasets_dataset_deleted.aiblock_id=datasets_data.aiblock_id

)

SELECT 
    final.datapoint_id,
    date_datapoint_uploaded,
    aiblock_id, 
    original_file_name,  
    old_user_id,
    user_id,
    account_id,
    remote_url,  
    storage_id,  
    text, 
    date_datapoint_updated
FROM 
    final
LEFT JOIN 
    data_deleted ON data_deleted.datapoint_id=final.datapoint_id
WHERE
    data_deleted.datapoint_id IS NULL