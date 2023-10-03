WITH mytable1 AS (
    SELECT * FROM {{ source('databricks_sample_data', 'mytable1') }}
), 

filtered AS (
    SELECT * FROM mytable1 
    WHERE state IN ('IN', 'NY')
),

final AS (
    SELECT * FROM filtered 
)


SELECT * FROM final 