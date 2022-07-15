WITH question_answered AS (

    SELECT 
        * 
    FROM {{ source('userflow', 'question_answered_view') }}

), users AS (

    SELECT 
        *
    FROM
        {{ref("users")}}
)

SELECT
    user_id,
    user_email_address,
    CASE
        WHEN text_answer = "Not a good time" THEN "Time investment too high"
        WHEN text_answer = "Pricing" THEN "Pricing"
        WHEN text_answer = "Just wanted to play around" THEN "Testing"
        WHEN text_answer = "I need a security feature" THEN "Privacy/Security"
        WHEN text_answer = "My team is not onboard" THEN "Not the Decision Maker"
        WHEN text_answer = "No data/wrong templates" THEN "No Training Data"
        WHEN text_answer = "Not a good fit for my use case" THEN "Use Case will never work"
        WHEN text_answer = "I'm missing a crucial feature" THEN "On Hold for Product Feature"
        WHEN text_answer = "It doesn't fit in my tech stack" THEN "Integration needs"  
        WHEN text_answer = "Other" THEN "Specific Reason (See Notes)"
    END AS hs_lead_status  
FROM 
    question_answered
INNER JOIN users USING(user_id)
WHERE 
    question_name = "cancellation reason"