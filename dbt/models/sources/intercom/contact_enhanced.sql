WITH source AS (

    SELECT * FROM {{ source('dbt_giancarlo_intercom', 'intercom__contact_enhanced') }}

)

SELECT 
    *
FROM 
    source