-- Create staging_events table
CREATE TABLE IF NOT EXISTS staging_events(
    artist VARCHAR,
    auth VARCHAR,
    firstName VARCHAR,
    gender VARCHAR,
    itemInSession INTEGER,
    lastName VARCHAR,
    length FLOAT,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration FLOAT,
    sessionId INTEGER,
    song VARCHAR,
    status INTEGER,
    ts BIGINT,
    userAgent VARCHAR,
    userId INTEGER
);

-- Create staging_songs table
CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs INTEGER,
    artist_id VARCHAR,
    artist_latitude DOUBLE PRECISION,
    artist_longitude DOUBLE PRECISION,
    artist_location VARCHAR,
    artist_name VARCHAR,
    song_id VARCHAR,
    title VARCHAR,
    duration DOUBLE PRECISION,
    year INTEGER
);

-- Create songplays table
CREATE TABLE IF NOT EXISTS songplays (
    playid varchar(32) NOT NULL,
    start_time timestamp NOT NULL,
    userid int4 NOT NULL,
    "level" varchar(256),
    songid varchar(256),
    artistid varchar(256),
    sessionid int4,
    location varchar(256),
    user_agent varchar(256),
    CONSTRAINT songplays_pkey PRIMARY KEY (playid)
);

-- Create users table
CREATE TABLE IF NOT EXISTS users(
    user_id             SERIAL          PRIMARY KEY,
    first_name          VARCHAR         NOT NULL,
    last_name           VARCHAR         NOT NULL,
    gender              VARCHAR(1)      NOT NULL,
    level               VARCHAR         NOT NULL
);

-- Create songs table
CREATE TABLE IF NOT EXISTS songs(
    song_id             VARCHAR         PRIMARY KEY,
    title               VARCHAR         NOT NULL,
    artist_id           VARCHAR         NOT NULL,
    year                INTEGER         NOT NULL,
    duration            NUMERIC
);

-- Create artists table
CREATE TABLE IF NOT EXISTS artists(
    artist_id           VARCHAR         PRIMARY KEY,
    name                VARCHAR         NOT NULL,
    location            VARCHAR,
    latitude            NUMERIC,
    longitude           NUMERIC
);

-- Create times table
CREATE TABLE IF NOT EXISTS times(
    start_time          TIMESTAMP       PRIMARY KEY,
    hour                INTEGER         NOT NULL,
    day                 INTEGER         NOT NULL,
    week                INTEGER         NOT NULL,
    month               INTEGER         NOT NULL,
    year                INTEGER         NOT NULL,
    weekday             VARCHAR(20)     NOT NULL
);
