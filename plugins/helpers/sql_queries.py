class SqlQueries:
    songplays_table_insert = ("""
        INSERT INTO songplays (playid, start_time, userid, level, songid, artistid, sessionid, location, user_agent)
        SELECT
            md5(events.sessionid::integer|| events.start_time::text) AS songplay_id,
            events.start_time, 
            events.userid, 
            events.level, 
            songs.song_id, 
            songs.artist_id, 
            events.sessionid, 
            events.location, 
            events.useragent
        FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
              FROM staging_events
              WHERE page='NextSong') events
        LEFT JOIN staging_songs songs
        ON events.song = songs.title
           AND events.artist = songs.artist_name
           AND events.length = songs.duration
        ON CONFLICT (playid) DO NOTHING;
    """)

    users_table_insert = ("""
        INSERT INTO users (user_id, first_name, last_name, gender, level)
        SELECT DISTINCT ON (userid) userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
        ON CONFLICT (user_id) DO UPDATE
        SET first_name = EXCLUDED.first_name,
            last_name = EXCLUDED.last_name,
            gender = EXCLUDED.gender,
            level = EXCLUDED.level
    """)

    songs_table_insert = ("""
        INSERT INTO songs (song_id, title, artist_id, year, duration)
        SELECT DISTINCT song_id, title, artist_id, year, duration
        FROM staging_songs
        ON CONFLICT (song_id) DO NOTHING
    """)

    artists_table_insert = ("""
        INSERT INTO artists (artist_id, name, location, latitude, longitude)
        SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
        ON CONFLICT (artist_id) DO NOTHING
    """)

    times_table_insert = ("""
        INSERT INTO times (start_time, hour, day, week, month, year, weekday)
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dow from start_time)
        FROM songplays
        ON CONFLICT (start_time) DO NOTHING
    """)
