WITH d_data AS (

    SELECT 
        new_user_id,
        new_dataset_id,
        new_id,
        created_at,
        new_workspace_id
    FROM 
        {{ref('normalized_d_data')}}

), userflow_ai_blocks AS (

    SELECT
        *
    FROM
        {{ref('userflow_ai_blocks')}}

), legacy_d_dataset AS (

    SELECT
        user_id AS new_user_id,
        workspace_id AS new_workspace_id,
        aiblock_id AS new_id,
        is_template AS new_is_template,
        aiblock_name AS new_name,
        aiblock_description AS new_description
    FROM 
        {{ref('datasets_dataset')}}
    WHERE
        date_aiblock_created<"2022-06-29 13:34:35.723000 UTC"

), d_dataset AS (

    SELECT
        new_user_id,
        new_workspace_id,
        new_id,
        new_is_template,
        new_name,
        new_description
    FROM
        {{ref('normalized_d_dataset')}}
    WHERE
        op = "INSERT"
    
), d_dataset_unioned AS (

    SELECT * FROM legacy_d_dataset UNION ALL
    SELECT * FROM d_dataset
    
)
    
SELECT 
    COALESCE(dd.new_user_id, ddu.new_user_id) AS user_id,
    CASE WHEN dd.new_user_id IS NULL THEN "yes" ELSE "no" END AS is_human_in_the_loop,
    dd.new_workspace_id AS workspace_id,
    dd.new_dataset_id AS dataset_id,                                       
    new_is_template AS is_template,
    COALESCE(is_userflow_data, "no") AS is_userflow_data,
    new_name AS dataset_name,
    new_description AS dataset_description,
    COUNT(dd.new_id) AS net_data_points, 
    MAX(dd.created_at) AS time_stamp
FROM d_data dd
INNER JOIN d_dataset_unioned ddu ON ddu.new_id = dd.new_dataset_id
LEFT JOIN userflow_ai_blocks uab ON ddu.new_id = uab.dataset_id
WHERE TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), dd.created_at, MINUTE)<10
GROUP BY 
    1, 
    2, 
    3, 
    4,
    5,
    6,
    7,
    8
