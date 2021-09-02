SELECT *
FROM (
  SELECT
      *,
      ROW_NUMBER()
          OVER (PARTITION BY ID)
          row_number
  FROM {{ref("pages")}}
)
WHERE row_number = 1