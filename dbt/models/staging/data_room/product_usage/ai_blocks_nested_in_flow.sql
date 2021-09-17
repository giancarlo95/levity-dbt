WITH workflows_classifierblock AS (

    SELECT 
        account_id,
        classifier_id,
        block_id,
        date_classifierblock_created
    FROM 
        {{ref('workflows_classifierblock')}}

), datasets_dataset AS (

    SELECT 
        aiblock_id,
        is_template
    FROM
        {{ref('datasets_dataset')}}

), prediction_models_classifier AS (

    SELECT 
        classifier_id,
        aiblock_id
    FROM 
        {{ref('prediction_models_classifier')}}

), prediction_models_classifier_enriched AS (

    SELECT 
        prediction_models_classifier.aiblock_id,
        classifier_id,
        is_template
    FROM prediction_models_classifier
    INNER JOIN datasets_dataset ON 
        prediction_models_classifier.aiblock_id=datasets_dataset.aiblock_id  

), onboarded_accounts AS (

    SELECT 
       *
    FROM 
       {{ref('onboarded_accounts')}}

)

SELECT 
    workflows_classifierblock.account_id,
    aiblock_id,
    is_template,
    CASE 
        WHEN onboarded_accounts.customer_status="Paid Plan" THEN "Paid Plan"
        ELSE "Design Partner"
    END                          AS customer_status_binary,
    MIN(date_classifierblock_created) AS date_first_nested
FROM workflows_classifierblock
INNER JOIN prediction_models_classifier_enriched ON 
    workflows_classifierblock.classifier_id=prediction_models_classifier_enriched.classifier_id 
INNER JOIN onboarded_accounts ON
    onboarded_accounts.account_id=workflows_classifierblock.account_id
GROUP BY 
    workflows_classifierblock.account_id,
    aiblock_id,
    is_template,
    CASE 
        WHEN onboarded_accounts.customer_status="Paid Plan" THEN "Paid Plan"
        ELSE "Design Partner"
    END 


    



