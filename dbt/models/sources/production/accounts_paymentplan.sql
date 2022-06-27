WITH source AS (

    SELECT * FROM {{ source('public', 'accounts_paymentplan') }}

),

renamed AS (

    SELECT
        *
    FROM source

)

SELECT *
FROM renamed