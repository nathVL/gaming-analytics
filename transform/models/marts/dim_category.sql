WITH
    categories_unnested AS (
        SELECT DISTINCT
            unnest(categories) AS category_name
        FROM
            {{ref ('stg_steam')}}
        WHERE
            categories IS NOT NULL
    )
SELECT
    row_number() OVER (
        ORDER BY
            category_name
    ) AS category_key,
    category_name
FROM
    categories_unnested