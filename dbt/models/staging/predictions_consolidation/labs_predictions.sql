WITH vetevo AS (

    SELECT * FROM {{ ref('vetevo') }}

)

SELECT 
    workspace_id,
    EXTRACT(DATE FROM date_prediction_made)            AS date,
    SUM(predictions_count)                             AS predictions_count
FROM 
    vetevo
GROUP BY
    workspace_id,
    EXTRACT(DATE FROM date_prediction_made)