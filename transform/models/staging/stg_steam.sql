WITH
    raw_steam AS (
        SELECT
            *
        FROM
            {{source ('raw_data', 'bronze_steam')}}
    )
SELECT
    appid::INT AS appid,
    name::VARCHAR AS name,
    strptime(release_date, '%d/%m/%Y') AS release_date,
    developers::VARCHAR[] AS developers,
    categories::VARCHAR[] AS categories,
    genre::VARCHAR[] AS genre,
    header_image::VARCHAR AS header_image,
    price / 100::DECIMAL(10, 2) AS price,
    currency::VARCHAR AS currency,
    windows::BOOLEAN AS windows,
    mac::BOOLEAN AS mac,
    linux::BOOLEAN AS linux,
    metacritic::INT AS metacritic,
    recommendations::INT AS recommendations
FROM
    raw_steam