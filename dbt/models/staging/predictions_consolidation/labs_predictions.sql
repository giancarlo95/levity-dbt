WITH vetevo AS (

    SELECT * FROM {{ ref('vetevo') }}

)

SELECT 
    account_id,
    EXTRACT(DATE FROM date_prediction_made)            AS date,
    SUM(predictions_count)                             AS predictions_count
FROM 
    vetevo
GROUP BY
    account_id,
    EXTRACT(DATE FROM date_prediction_made)