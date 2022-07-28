WITH legacy_pm_classifier AS (

    SELECT
        user_id AS new_user_id,
        aiblock_id AS new_dataset_id,
        classifier_id AS new_id
    FROM 
        {{ref('prediction_models_classifier')}}
    WHERE
        created_at<"2022-06-29 13:34:35.727000 UTC"

), pm_classifier AS (

    SELECT
        new_user_id,
        new_dataset_id,
        new_id
    FROM
        {{ref('normalized_pm_classifier')}}
    WHERE
        op = "INSERT"
    
), pm_classifier_unioned AS (

    SELECT * FROM legacy_pm_classifier UNION ALL
    SELECT * FROM pm_classifier
    
), pm_prediction AS (

    SELECT
        new_user_id,
        new_workspace_id,
        new_id,
        new_is_hitl,
        CASE WHEN NOT(new_source LIKE "flows%" OR new_source LIKE "Integromat%" OR new_source LIKE "Zapier%" OR new_source = "test_tab" OR new_source LIKE "Bubble%") THEN "API" ELSE new_source END AS new_source,
        new_workflow_id,
        created_at,
        new_classifier_id
    FROM 
        {{ref('normalized_pm_prediction')}}

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
    COALESCE(pmp.new_user_id, pmcu.new_user_id) AS user_id,
    pmp.new_workspace_id AS workspace_id,
    pmcu.new_dataset_id AS dataset_id,
    new_is_template AS is_template,
    new_name AS dataset_name,
    new_description AS dataset_description,
    new_is_hitl AS is_hitl,
    is_userflow_data AS is_userflow_data,
    new_source AS new_origin,
    new_workflow_id AS workflow_id,
    COUNT(pmp.new_id) AS total_predictions,
    MAX(pmp.created_at) AS time_stamp
FROM pm_prediction pmp
INNER JOIN pm_classifier_unioned pmcu ON pmp.new_classifier_id = pmcu.new_id
INNER JOIN d_dataset_unioned ddu ON ddu.new_id=pmcu.new_dataset_id
LEFT JOIN userflow_ai_blocks uab ON ddu.new_id = uab.dataset_id
WHERE TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), pmp.created_at, MINUTE) < 10
GROUP BY 
    1, 
    2, 
    3, 
    4,
    5,
    6,
    7,
    8,
    9,
    10
