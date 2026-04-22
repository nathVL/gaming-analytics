WITH
    game_genres AS (
        SELECT
            s.appid,
            unnest(s.genre) AS genre_name
        FROM
            {{ref ('stg_steam')}} s
        WHERE
            s.genre IS NOT NULL
    )
SELECT
    g.game_key,
    ge.genre_key
FROM
    game_genres gg
    INNER JOIN {{ref ('dim_games')}} g ON gg.appid = g.appid
    INNER JOIN {{ref ('dim_genre')}} ge ON gg.genre_name = ge.genre_name