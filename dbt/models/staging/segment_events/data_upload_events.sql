WITH datasets_data AS (

    SELECT 
        user_id,
        aiblock_id,
        datapoint_id,
        date_datapoint_uploaded,
        workspace_id
    FROM 
        {{ref('datasets_data')}}

), userflow_ai_blocks AS (

    SELECT
        *
    FROM
        {{ref('userflow_ai_blocks')}}

), datasets_dataset AS (

    SELECT
        user_id,
        workspace_id,
        aiblock_id,
        is_template,
        aiblock_name,
        aiblock_description,
        COALESCE(is_userflow_data, "no") AS is_userflow_data
    FROM {{ref('datasets_dataset')}} dd
    LEFT JOIN userflow_ai_blocks uab ON dd.aiblock_id = uab.dataset_id

), workspaces AS (

    SELECT
        workspace_id
    FROM
        {{ref('workspaces')}}
        
), users AS (

    SELECT 
        user_id,
        user_email_address
    FROM
        {{ref('users')}}
    
), final AS (
    
    SELECT 
        IFNULL(dsd.user_id, dst.user_id)                         AS user_id,
        CASE 
            WHEN dsd.user_id IS NULL THEN "yes"
            ELSE "no"
        END                                                      AS is_human_in_the_loop,
        dsd.workspace_id,
        dsd.aiblock_id                                           AS aiblock_id,
        is_template,
        is_userflow_data,
        aiblock_name,
        aiblock_description,
        TIMESTAMP_TRUNC(date_datapoint_uploaded, HOUR)           AS relevant_day_hour,
        COUNT(dsd.datapoint_id)                                  AS net_data_points, 
        MAX(dsd.date_datapoint_uploaded)                         AS time_stamp,
    FROM datasets_data dsd
    INNER JOIN datasets_dataset dst ON dsd.aiblock_id = dst.aiblock_id
    WHERE TIMESTAMP_TRUNC(date_datapoint_uploaded, HOUR) = TIMESTAMP_TRUNC(TIMESTAMP_SUB(CURRENT_TIMESTAMP(),INTERVAL 2 HOUR), HOUR)
    GROUP BY 
        1, 
        2, 
        3, 
        4,
        5,
        6,
        7,
        8,
        9

)

SELECT 
    final.user_id,
    u.user_email_address,
    is_human_in_the_loop,
    final.workspace_id,
    aiblock_id,
    is_template,
    is_userflow_data,
    aiblock_name,
    aiblock_description,
    net_data_points,
    time_stamp
FROM final
INNER JOIN workspaces ob ON final.workspace_id = ob.workspace_id
INNER JOIN users u ON u.user_id=final.user_id

