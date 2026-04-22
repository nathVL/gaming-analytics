WITH
    raw_twitch AS (
        SELECT
            *
        FROM
            {{source ('raw_data', 'bronze_twitch')}}
    )
SELECT
    steam_id::INT AS appid,
    Month::DATE AS metric_month,
    AvgViewers::INT AS avg_viewers,
    AvgViewersGain::INT AS avg_viewers_gain,
    "AvgViewers%Gain"::DECIMAL(10, 1) AS avg_viewers_pct_gain,
    PeakViewers::INT AS peak_viewers,
    AvgStreams::INT AS avg_streams,
    AvgStreamsGain::INT AS avg_streams_gain,
    "AvgStreamsùGain"::DECIMAL(10, 1) AS avg_streams_u_gain,
    PeakStreams::INT AS peak_streams,
    HoursWatched::INT AS hours_watched
FROM
    raw_twitch