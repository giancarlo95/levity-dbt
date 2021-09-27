{{
  config(
    materialized = 'table',
    )
}}

WITH funnel_chart_updated AS (

    SELECT 
        Number_of_customers,
        Funnel_step
    FROM {{ref('funnel_chart_updated_account')}}

), first_step AS (

SELECT
    Funnel_step,
    Number_of_customers/(SELECT Number_of_customers FROM funnel_chart_updated WHERE Funnel_step='A.Onboarded accounts') AS Relative_number,
FROM 
    funnel_chart_updated
WHERE 
    Funnel_step='B.Created at least 1 AI Block'
ORDER BY
    Funnel_step

), second_step AS (

SELECT
    Funnel_step,
    Number_of_customers/(SELECT Number_of_customers FROM funnel_chart_updated WHERE Funnel_step='B.Created at least 1 AI Block') AS Relative_number,
FROM 
    funnel_chart_updated
WHERE 
    Funnel_step='C.Uploaded at least 1 Data point to at least 1 AI Block'
ORDER BY
    Funnel_step

), third_step AS (

SELECT
    Funnel_step,
    Number_of_customers/(SELECT Number_of_customers FROM funnel_chart_updated WHERE Funnel_step='C.Uploaded at least 1 Data point to at least 1 AI Block') AS Relative_number,
FROM 
    funnel_chart_updated
WHERE 
    Funnel_step='D.Uploaded at least 40 Data points to at least 1 AI Block'
ORDER BY
    Funnel_step

), fourth_step AS (

SELECT
    Funnel_step,
    Number_of_customers/(SELECT Number_of_customers FROM funnel_chart_updated WHERE Funnel_step='D.Uploaded at least 40 Data points to at least 1 AI Block') AS Relative_number,
FROM 
    funnel_chart_updated
WHERE 
    Funnel_step='E.Trained at least 1 AI Block'
ORDER BY
    Funnel_step

), fifth_step AS (

SELECT
    Funnel_step,
    Number_of_customers/(SELECT Number_of_customers FROM funnel_chart_updated WHERE Funnel_step='E.Trained at least 1 AI Block') AS Relative_number,
FROM 
    funnel_chart_updated
WHERE 
    Funnel_step='F.Made at least 1 Prediction through at least 1 AI Block'
ORDER BY
    Funnel_step

), sixth_step AS (

SELECT
    Funnel_step,
    Number_of_customers/(SELECT Number_of_customers FROM funnel_chart_updated WHERE Funnel_step='F.Made at least 1 Prediction through at least 1 AI Block') AS Relative_number,
FROM 
    funnel_chart_updated
WHERE 
    Funnel_step='G.Made at least 50 Predictions through at least 1 AI Block'
ORDER BY
    Funnel_step

)

SELECT * FROM first_step UNION ALL
SELECT * FROM second_step UNION ALL
SELECT * FROM third_step UNION ALL
SELECT * FROM fourth_step UNION ALL
SELECT * FROM fifth_step UNION ALL
SELECT * FROM sixth_step
ORDER BY Funnel_step



