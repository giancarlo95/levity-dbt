{{
  config(
    materialized = 'table',
    )
}}

WITH a_paymentplan_created AS (

    SELECT
        new_user_id,
        new_plan_id,
        DATE_ADD(new_period_start, INTERVAL CAST(new_trial_period AS INT) DAY) AS trial_end
    FROM
        (SELECT 
            *,
            ROW_NUMBER() OVER(PARTITION BY new_user_id ORDER BY created_at DESC) AS index
        FROM
            {{ref("normalized_a_paymentplan")}}
        WHERE
            op = "INSERT"
            AND new_plan_id IS NOT NULL) AS creation
    WHERE 
        index = 1

), a_paymentplan_deleted AS (

    SELECT 
        new_user_id,
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

), users AS (

    SELECT
        *
    FROM
        {{ref("users")}}
)

SELECT 
    new_user_id AS user_id,
    user_email_address AS email,
    "yes" AS is_onboarded,
    CASE
        WHEN DATE_DIFF(CURRENT_DATE(), trial_end, DAY)>1 THEN "customer"
        ELSE "salesqualifiedlead"
    END AS lifecyclestage,
    CASE
        WHEN DATE_DIFF(CURRENT_DATE(), trial_end, DAY)>1 THEN "Paid Plan"
        ELSE "Active in free plan"
    END AS hs_lead_status,
    new_plan_id AS subscription_plan_id
FROM
    a_paymentplan_created apc
LEFT JOIN a_paymentplan_deleted apd USING (new_user_id)
INNER JOIN users u ON u.user_id = apc.new_user_id
WHERE
    apd.new_user_id IS NULL


