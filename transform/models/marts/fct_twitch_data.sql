WITH
    twitch AS (
        SELECT
            *
        FROM
            {{ref ('stg_twitch')}}
    )
SELECT
    g.game_key,
    d.date_key,
    t.avg_viewers,
    t.avg_viewers_gain,
    t.avg_viewers_pct_gain,
    t.peak_viewers,
    t.avg_streams,
    t.avg_streams_gain,
    t.avg_streams_u_gain,
    t.peak_streams,
    t.hours_watched
FROM
    twitch t
    INNER JOIN {{ref ('dim_games')}} g ON t.appid = g.appid
    INNER JOIN {{ref ('dim_date')}} d ON (
        year(t.metric_month) * 100 + month(t.metric_month)
    )::INT = d.date_key