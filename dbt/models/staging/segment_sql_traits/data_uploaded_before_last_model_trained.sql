WITH datasets_data AS (

    SELECT 
        user_id,
        aiblock_id,
        datapoint_id,
        date_datapoint_uploaded,
        account_id
    FROM 
        {{ref('datasets_data')}}

), datasets_dataset AS (

    SELECT
        user_id,
        aiblock_id
    FROM 
        {{ref('datasets_dataset')}}

), prediction_models_trainingrun AS (

    SELECT 
        user_id,
        date_training_run
    FROM
        {{ref('prediction_models_trainingrun')}}
    WHERE 
        version_id IS NOT NULL
        
), last_trainings AS (

    SELECT
        user_id,
        MAX(date_training_run) AS date_last_training_run
    FROM 
        prediction_models_trainingrun
    GROUP BY
        user_id

), final AS (
    
    SELECT 
        IFNULL(dsd.user_id, dst.user_id)    AS user_id,
        dsd.datapoint_id,
        date_datapoint_uploaded
    FROM datasets_data dsd
    INNER JOIN datasets_dataset dst ON dsd.aiblock_id = dst.aiblock_id

)

SELECT 
    f.user_id,
    COUNT(datapoint_id) AS net_data_points
FROM
    final f
INNER JOIN last_trainings lt ON lt.user_id=f.user_id
WHERE TIMESTAMP_DIFF(date_datapoint_uploaded, date_last_training_run, MINUTE)<=0
GROUP BY
    f.user_id


