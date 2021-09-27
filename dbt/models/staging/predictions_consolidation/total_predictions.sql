WITH labs_predictions AS (

    SELECT * FROM {{ ref('labs_predictions') }}

), production_predictions AS (

    SELECT * FROM {{ ref('production_predictions') }}

)

SELECT * FROM labs_predictions UNION ALL
SELECT * FROM production_predictions


