WITH source AS (

    SELECT * FROM {{ source('legacy', 'first_typeform_2021')}}

), renamed AS (

    SELECT
        id,
        email,
        DATE(submit_date) AS submit_date,
        is_company_email,
        ROW_NUMBER() OVER (PARTITION BY email ORDER BY submit_date ASC) AS index
    FROM 
        source


)

SELECT *
FROM renamed
WHERE 
    index=1