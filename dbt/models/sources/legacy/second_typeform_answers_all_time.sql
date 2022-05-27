WITH source AS (

    SELECT * FROM {{ source('legacy', 'second_typeform_answers_all_time')}}

), renamed AS (

    SELECT
        * EXCEPT(submit_date),
        DATE(submit_date) AS submit_date,
        CASE 
            WHEN company_size IN ("Just me", "2-10") OR never_used_automation_tools = 1 OR data_type IN ("Video or Sound", "Structured Data (e.g. Date, Name, Location, Monthly Sales)") OR data_type IS NULL OR company_size IS NULL THEN 0
            ELSE 1
        END AS is_high_score
    FROM 
        source

)

SELECT *
FROM renamed