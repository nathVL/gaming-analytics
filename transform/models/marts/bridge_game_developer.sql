WITH
    game_developers AS (
        SELECT
            s.appid,
            unnest(s.developers) AS developer_name
        FROM
            {{ref ('stg_steam')}} s
        WHERE
            s.developers IS NOT NULL
    )
SELECT
    g.game_key,
    d.developer_key
FROM
    game_developers gd
    INNER JOIN {{ref ('dim_games')}} g ON gd.appid = g.appid
    INNER JOIN {{ref ('dim_developer')}} d ON gd.developer_name = d.developer_name