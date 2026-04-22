WITH
    game_categories AS (
        SELECT
            s.appid,
            unnest(s.categories) AS category_name
        FROM
            {{ref ('stg_steam')}} s
        WHERE
            s.categories IS NOT NULL
    )
SELECT
    g.game_key,
    c.category_key
FROM
    game_categories gc
    INNER JOIN {{ref ('dim_games')}} g ON gc.appid = g.appid
    INNER JOIN {{ref ('dim_category')}} c ON gc.category_name = c.category_name