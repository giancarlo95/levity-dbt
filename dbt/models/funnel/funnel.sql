WITH onboarded_users AS (

    SELECT * FROM {{ref('onboarded_users')}}
       
), created_ai_block AS (

    SELECT * FROM {{ref('created_ai_block')}}

), uploaded_data AS (

    SELECT * FROM {{ref('uploaded_data')}}

), uploaded_40datapoints AS (

    SELECT * FROM {{ref('uploaded_40datapoints')}}

), trained_ai_block AS (

    SELECT * FROM {{ref('trained_ai_block')}}

), made_prediction AS (

    SELECT * FROM {{ref('made_prediction')}}

), final AS (

    SELECT 
        onboarded_users.user_id,
        onboarded_users.date_user_onboarded,
        created_ai_block.date_first_aiblock_created,
        uploaded_data.date_first_somedata_uploaded,
        uploaded_40datapoints.date_first_40datapoints_uploaded,
        trained_ai_block.date_first_training_run,
        made_prediction.date_first_prediction_made
    FROM onboarded_users
    LEFT JOIN created_ai_block ON 
        onboarded_users.user_id=created_ai_block.user_id
    LEFT JOIN uploaded_data ON
        onboarded_users.user_id=uploaded_data.user_id
    LEFT JOIN uploaded_40datapoints ON
        onboarded_users.user_id=uploaded_40datapoints.user_id
    LEFT JOIN trained_ai_block ON
        onboarded_users.user_id=trained_ai_block.user_id
    LEFT JOIN made_prediction ON
        onboarded_users.user_id=made_prediction.user_id    

), final_transposed AS (

    SELECT 'A.Onboarded companies' AS Step, (SELECT COUNT(date_user_onboarded) FROM final) AS Count UNION ALL
    SELECT 'B.Created at least 1 AI Block' AS Step, (SELECT COUNT(date_first_aiblock_created) FROM final) AS Count UNION ALL
    SELECT 'C.Uploaded data to at least 1 AI Block' AS Step, (SELECT COUNT(date_first_somedata_uploaded) FROM final) AS Count UNION ALL
    SELECT 'D.Uploaded at least 40 data points to at least 1 AI Block' AS Step, (SELECT COUNT(date_first_40datapoints_uploaded) FROM final) AS Count UNION ALL
    SELECT 'E.Trained at least 1 AI Block' AS Step, (SELECT COUNT(date_first_training_run) FROM final) AS Count UNION ALL
    SELECT 'F.Made at least 1 prediction through at least 1 AI Block' AS Step, (SELECT COUNT(date_first_prediction_made) FROM final) AS Count

)

SELECT 
    * 
FROM 
    final_transposed 
ORDER BY 
    Step