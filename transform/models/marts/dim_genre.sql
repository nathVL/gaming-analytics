WITH
    genres_unnested AS (
        SELECT DISTINCT
            unnest(genre) AS genre_name
        FROM
            {{ref ('stg_steam')}}
        WHERE
            genre IS NOT NULL
    )
SELECT
    row_number() OVER (
        ORDER BY
            genre_name
    ) AS genre_key,
    genre_name
FROM
    genres_unnested