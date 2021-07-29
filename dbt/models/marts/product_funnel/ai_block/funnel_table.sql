{{
  config(
    materialized = 'table',
    )
}}

WITH onboarded_users AS (

    SELECT * FROM {{ref('onboarded_users')}}
       
), created_ai_block AS (

    SELECT * FROM {{ref('created_ai_block')}}

), uploaded_data AS (

    SELECT * FROM {{ref('uploaded_data')}}

), uploaded_labeldata AS (

    SELECT * FROM {{ref('uploaded_labeldata')}}

), uploaded_40datapoints AS (

    SELECT * FROM {{ref('uploaded_40datapoints')}}

), uploaded_40labeldatapoints AS (

    SELECT * FROM {{ref('uploaded_40labeldatapoints')}}

), trained_ai_block AS (

    SELECT * FROM {{ref('trained_ai_block')}}

), made_prediction AS (

    SELECT * FROM {{ref('made_prediction')}}

), made_50predictions AS (

    SELECT * FROM {{ref('made_50predictions')}}

) 

SELECT 
    onboarded_users.user_id,
    onboarded_users.user_email_address,
    onboarded_users.date_user_onboarded,
    created_ai_block.date_first_aiblock_created,
    uploaded_data.date_first_somedata_uploaded,
    uploaded_labeldata.date_first_labelled_datapoint_uploaded,
    uploaded_40datapoints.date_first_40datapoints_uploaded,
    uploaded_40labeldatapoints.date_first_40labeldatapoints_uploaded,
    trained_ai_block.date_first_training_run,
    made_prediction.date_first_prediction_made,
    made_50predictions.date_first_50predictions_made
FROM onboarded_users
LEFT JOIN created_ai_block ON 
    onboarded_users.user_id=created_ai_block.user_id
LEFT JOIN uploaded_data ON
    onboarded_users.user_id=uploaded_data.user_id
LEFT JOIN uploaded_labeldata ON
    onboarded_users.user_id=uploaded_labeldata.user_id
LEFT JOIN uploaded_40datapoints ON
    onboarded_users.user_id=uploaded_40datapoints.user_id
LEFT JOIN uploaded_40labeldatapoints ON
    onboarded_users.user_id=uploaded_40labeldatapoints.user_id
LEFT JOIN trained_ai_block ON
    onboarded_users.user_id=trained_ai_block.user_id
LEFT JOIN made_prediction ON
    onboarded_users.user_id=made_prediction.user_id 
LEFT JOIN made_50predictions ON
    onboarded_users.user_id=made_50predictions.user_id    
