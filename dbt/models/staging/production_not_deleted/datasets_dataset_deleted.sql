WITH datasets_dataset AS (

    SELECT
       * 
    FROM 
       {{ref('datasets_dataset')}}

), dataset_deleted AS (

    SELECT 
       *
    FROM 
       {{ref('dataset_deleted')}}

)

SELECT 
    datasets_dataset.aiblock_id,   			
    date_aiblock_created,	
    aiblock_description,
    is_template,		
    old_user_id,
    user_id,
    account_id,			
    aiblock_data_type,	
    date_aiblock_updated
FROM 
    datasets_dataset
LEFT JOIN 
    dataset_deleted ON dataset_deleted.aiblock_id=datasets_dataset.aiblock_id
WHERE
    dataset_deleted.aiblock_id IS NULL