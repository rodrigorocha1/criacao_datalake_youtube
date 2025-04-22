create external table bronze_videos(
    kind STRING,
    etag STRING,
    items ARRAY<STRUCT<
        kind: STRING,
        etag: STRING,
        id: STRING,
        snippet: STRUCT<
            publishedAt: STRING,
            channelId: STRING,
            title: STRING,
            description: STRING,
            thumbnails: STRUCT<
                `default`: STRUCT<url: STRING, width: INT, height: INT>,
                medium: STRUCT<url: STRING, width: INT, height: INT>,
                high: STRUCT<url: STRING, width: INT, height: INT>,
                standard: STRUCT<url: STRING, width: INT, height: INT>,
                maxres: STRUCT<url: STRING, width: INT, height: INT>
            >,
            channelTitle: STRING,
            tags: ARRAY<STRING>,
            categoryId: STRING,
            liveBroadcastContent: STRING,
            localized: STRUCT<title: STRING, description: STRING>,
            defaultAudioLanguage: STRING
        >,
        contentDetails: STRUCT<
            duration: STRING,
            dimension: STRING,
            definition: STRING,
            caption: STRING,
            licensedContent: BOOLEAN,
            contentRating: MAP<STRING, STRING>,
            projection: STRING
        >,
        status: STRUCT<
            uploadStatus: STRING,
            privacyStatus: STRING,
            license: STRING,
            embeddable: BOOLEAN,
            publicStatsViewable: BOOLEAN,
            madeForKids: BOOLEAN
        >,
        statistics: STRUCT<
            viewCount: BIGINT,  -- Using STRING as BIGINT might cause issues with large numbers
            likeCount: BIGINT,
            favoriteCount: BIGINT,
            commentCount: BIGINT
        >
    >>,
    pageInfo STRUCT<
        totalResults: INT,
        resultsPerPage: INT
    >
)
PARTITIONED BY (ano INT, mes INT, dia INT, dia_semana string, assunto STRING)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION 'file:///datalake/bronze/videos';