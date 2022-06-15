WITH source AS (

    SELECT * FROM {{ source('transfer', 'p_AccountBasicStats_7097664393')}}

), renamed AS (

    SELECT
        *
    FROM 
        source
        
    {# ORDER BY
        year_month DESC #}

)

SELECT *
FROM renamed