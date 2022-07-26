WITH question_answered AS (

    SELECT 
        * 
    FROM {{ source('userflow', 'question_answered_view') }}

), users AS (

    SELECT 
        *
    FROM
        {{ref("users")}}

), answer_given AS (

    SELECT
        user_id,
        text_answer,
        CASE
            WHEN text_answer = "Timing problem" THEN "Time investment too high"
            WHEN text_answer = "Pricing" THEN "Pricing"
            WHEN text_answer = "Testing" THEN "Testing"
            WHEN text_answer = "Privacy/Security" THEN "Privacy/Security"
            WHEN text_answer = "Not the decision maker" THEN "Not the Decision Maker"
            WHEN text_answer = "No training data" THEN "No Training Data"
            WHEN text_answer = "Will never work" THEN "Use Case will never work"
            WHEN text_answer = "On Hold for the Product feature" THEN "On Hold for Product Feature"
            WHEN text_answer = "Integration needs" THEN "Integration needs"  
            WHEN text_answer = "Other" THEN "Specific Reason (See Notes)"
            ELSE "Specific Reason (See Notes)"
        END AS hs_lead_status  
    FROM 
        question_answered
    WHERE 
        question_name = "cancellation reason"

), a_paymentplan_deleted AS (

    SELECT 
        new_user_id AS user_id,
        new_workspace_id AS workspace_id,
        new_status
    FROM 
        (SELECT 
            *,
            ROW_NUMBER() OVER(PARTITION BY new_user_id ORDER BY created_at DESC) AS index
        FROM
            {{ref("normalized_a_paymentplan")}}) AS deletion
    WHERE
        index = 1 
        AND op = "UPDATE"
        AND new_status = "deleted"

)

SELECT
    user_id,
    user_email_address AS email,
    COALESCE(hs_lead_status, "Retarget") AS hs_lead_status
FROM
    a_paymentplan_deleted
LEFT JOIN answer_given USING(user_id)
INNER JOIN users USING(user_id)
