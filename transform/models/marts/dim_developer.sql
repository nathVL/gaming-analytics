WITH
    developers_unnested AS (
        SELECT DISTINCT
            unnest(developers) AS developer_name
        FROM
            {{ref ('stg_steam')}}
        WHERE
            developers IS NOT NULL
    )
SELECT
    row_number() OVER (
        ORDER BY
            developer_name
    ) AS developer_key,
    developer_name
FROM
    developers_unnested