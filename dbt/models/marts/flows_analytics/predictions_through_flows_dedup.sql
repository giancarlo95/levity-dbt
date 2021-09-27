WITH predictions_through_flows AS (

    SELECT 
        * 
    FROM 
        {{ref('predictions_through_flows')}}

), predictions_through_flows_partitioned AS (
    
    SELECT 
        *,
        DATE(date_prediction_made)                    AS date_prediction_made_dateformat,
        ROW_NUMBER() OVER(PARTITION BY prediction_id) AS screen
    FROM
        predictions_through_flows

) 

SELECT 
    *
FROM 
    predictions_through_flows_partitioned
WHERE 
    screen=1


