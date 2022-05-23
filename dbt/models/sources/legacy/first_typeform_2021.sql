WITH source AS (

    SELECT * FROM {{ source('legacy', 'first_typeform_2021')}}

), renamed AS (

    SELECT
        id,
        email,
        DATE(submit_date) AS submit_date,
        is_company_email
    FROM 
        source

)

SELECT *
FROM renamed