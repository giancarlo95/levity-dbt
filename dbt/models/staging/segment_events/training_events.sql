WITH users AS (

    SELECT 
        user_id,
        user_email_address
    FROM
        {{ref('users')}}

), userflow_ai_blocks AS (

    SELECT
        *
    FROM
        {{ref('userflow_ai_blocks')}}

), d_dataset AS (

    SELECT 
        * 
    FROM 
        {{ref('normalized_d_dataset')}} ndd
    LEFT JOIN userflow_ai_blocks uab ON ndd.new_id = uab.dataset_id 
    WHERE 
        op = "INSERT"

), pm_classifier AS (

    SELECT 
        *
    FROM 
        {{ref('normalized_pm_classifier')}}
    WHERE   
        op = "INSERT"

), pm_classifierversion AS (

    SELECT 
        *
    FROM 
        {{ref('normalized_pm_classifierversion')}}
    WHERE
        op = "UPDATE"
        AND new_status = "ready"
        AND new_training_progress = 100
        AND NOT(CONCAT(new_status, CAST(new_training_progress AS STRING)) = CONCAT(old_status, CAST(old_training_progress AS STRING)))

)

SELECT 
    u.user_email_address,
    pmcv.new_user_id,
    pmcv.new_workspace_id,
    pmcv.new_id AS classifierversion_id,
    dd.new_id AS dataset_id,
    CASE
        WHEN dd.new_description IS NULL THEN "no"
        ELSE "yes"
    END AS is_template_retraining,
    COALESCE(is_userflow_data, "no") AS is_userflow_data,
    new_performance_score AS performance_score,
    pmcv.created_at AS time_stamp
FROM 
    pm_classifierversion pmcv
INNER JOIN pm_classifier pmc ON pmc.new_id = pmcv.new_classifier_id
INNER JOIN d_dataset dd ON dd.new_id = pmc.new_dataset_id
INNER JOIN users u ON u.user_id = pmcv.new_user_id
WHERE TIMESTAMP_TRUNC(pmcv.created_at, HOUR) = TIMESTAMP_TRUNC(TIMESTAMP_SUB(CURRENT_TIMESTAMP(),INTERVAL 2 HOUR), HOUR)



