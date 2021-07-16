{{
  config(
    materialized = 'table',
    )
}}

WITH funnel_chart_lag AS (

    SELECT 
        Number_of_customers AS Number_of_customers_lag,
        Funnel_step
    FROM {{ref('funnel_chart_lag')}}
       
), funnel_chart_updated AS (

    SELECT 
        Number_of_customers AS Number_of_customers_current,
        Funnel_step
    FROM {{ref('funnel_chart_updated')}}

), funnel_chart_recent AS (

    SELECT 
        Number_of_customers AS Number_of_customers_recent,
        Funnel_step
    FROM {{ref('funnel_chart_recent')}}

), funnel_chart_combined AS (

    SELECT 
        funnel_chart_lag.Funnel_step,
        Number_of_customers_lag,
        Number_of_customers_current,
        Number_of_customers_recent AS Users_Acquired_in_the_Last_Month,
        (Number_of_customers_current-Number_of_customers_lag)/Number_of_customers_lag AS Growth_rate,
        Number_of_customers_current-Number_of_customers_lag-Number_of_customers_recent AS Users_Acquired_More_than_One_Month_Ago
    FROM funnel_chart_lag 
        LEFT JOIN funnel_chart_updated
        ON funnel_chart_lag.Funnel_step=funnel_chart_updated.Funnel_step
        LEFT JOIN funnel_chart_recent
        ON funnel_chart_lag.Funnel_step=funnel_chart_recent.Funnel_step   

)

SELECT
    Funnel_step,
    Growth_rate,
    (SELECT Growth_rate FROM funnel_chart_combined WHERE Funnel_step='A.Onboarded users') AS Comparison_term,
    Growth_rate - (SELECT Growth_rate FROM funnel_chart_combined WHERE Funnel_step='A.Onboarded users') AS Relative_growth,
    Users_Acquired_in_the_Last_Month,
    Users_Acquired_More_than_One_Month_Ago
FROM 
    funnel_chart_combined
WHERE 
    NOT(Funnel_step='A.Onboarded users')
ORDER BY
    Funnel_step

