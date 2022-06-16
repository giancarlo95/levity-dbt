WITH source AS (

    SELECT * FROM {{ source('transfer', 'p_AccountBasicStats_7097664393')}}

), renamed AS (

    SELECT
       *			
    FROM 
        source

)

SELECT *
FROM renamed