WITH
    games AS (
        SELECT
            *
        FROM
            {{ref ('stg_steam')}}
    )
SELECT
    row_number() OVER (
        ORDER BY
            appid
    ) AS game_key,
    appid,
    name,
    release_date,
    price,
    currency,
    windows,
    mac,
    linux,
    metacritic,
    recommendations,
    header_image
FROM
    games