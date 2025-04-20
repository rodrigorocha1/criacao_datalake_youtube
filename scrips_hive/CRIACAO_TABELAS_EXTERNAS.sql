-- Bronze ---------------------------------------------------


drop table bronze_assunto;

select *
from bronze_assunto;

describe  bronze_assunto

alter table bronze_assunto
add partition (ano=2024,mes=1,dia=1,dia_semana='segunda-feira' , assunto="Teste" );


ALTER TABLE bronze_assunto DROP PARTITION (ano=2024,mes=1,dia=1,dia_semana='segunda-feira' , assunto="Teste");

show partitions bronze_assunto;

create external table bronze_assunto (
	kind string,
	etag string,
	nextPageToken CHAR(6),
	prevPageToken char(6),
	regionCode CHAR(2),
	pageInfo struct<totalResults:INT, resultsPerPage:INT>,
	items ARRAY<STRUCT<
    kind: STRING,
    etag: STRING,
    id: STRUCT<
      kind: STRING,
      videoId: STRING
    >,
    snippet: STRUCT<
      publishedAt: STRING,
      channelId: STRING,
      title: STRING,
      description: STRING,
      thumbnails: STRUCT<
        default: STRUCT<
          url: STRING,
          width: INT,
          height: INT
        >,
        medium: STRUCT<
          url: STRING,
          width: INT,
          height: INT
        >,
        high: STRUCT<
          url: STRING,
          width: INT,
          height: INT
        >
      >,
      channelTitle: STRING,
      liveBroadcastContent: STRING,
      publishTime: STRING
    >
  >>
	
)
PARTITIONED BY (ano INT, mes INT, dia INT, dia_semana string, assunto STRING)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION 'file:///datalake/bronze/assunto';







create external table bronze_canais  (
    kind STRING,
    etag STRING,
    page_info STRUCT<
        total_results: INT,
        results_per_page: INT
    >,
    items ARRAY<STRUCT<
        kind: STRING,
        etag: STRING,
        id: STRING,
        snippet: STRUCT<
            title: STRING,
            description: STRING,
            custom_url: STRING,
            published_at: STRING,
            thumbnails: STRUCT<
                default: STRUCT<url: STRING, width: INT, height: INT>,
                medium: STRUCT<url: STRING, width: INT, height: INT>,
                high: STRUCT<url: STRING, width: INT, height: INT>
            >,
            default_language: STRING,
            localized: STRUCT<
                title: STRING,
                description: STRING
            >,
            country: STRING
        >,
        statistics: STRUCT<
            view_count: STRING,
            subscriber_count: STRING,
            hidden_subscriber_count: BOOLEAN,
            video_count: STRING
        >
    >>
)
PARTITIONED BY (ano INT, mes INT, dia INT, dia_semana string, assunto STRING)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION 'file:///datalake/bronze/canais';




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



