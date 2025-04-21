-- Bronze ---------------------------------------------------

select * 
from canais c ;

select *
from videos v ;

ALTER TABLE bronze_assunto
                        ADD IF NOT EXISTS PARTITION (
                            ano=2025,
                            mes=4,
                            dia=21,
                            dia_semana='Segunda-feira',
                            assunto="python"
                        )
                        
drop table bronze_assunto ; 

insert into canais 
partition(assunto='teste')
VALUES('a', 'b');

drop table bronze_assunto;

select *
from bronze_assunto;

describe  bronze_assunto

alter table bronze_assunto
DROP partition (ano=2025,
                            mes=4,
                            dia=21,
                            dia_semana='Segunda-feira',
                            assunto="python" );


                        ALTER TABLE bronze_assunto
                        ADD  PARTITION (
                            ano=2025,
                            mes=4,
                            dia=21,
                            dia_semana='Segunda-feira',
                            assunto="python"
                        )
                        
SHOW PARTITIONS bronze_assunto PARTITION (ano=2025, mes=4, dia=21, dia_semana='Segunda-feira', assunto="python")


ALTER TABLE bronze_assunto DROP PARTITION (ano=2025, mes=4, dia=21, dia_semana='Segunda-feira', assunto="python");

show partitions bronze_assunto;

drop table bronze_assunto

select ba.snippet.channelid
from bronze_assunto ba

create external table bronze_assunto (
    kind STRING,
    etag STRING,
    id STRUCT<
        kind: STRING,
        videoId: STRING
    >,
    snippet STRUCT<
        publishedAt: TIMESTAMP,
        channelId: STRING,
        title: STRING,
        description: STRING,
        thumbnails: STRUCT<
            default: STRUCT<url: STRING, width: INT, height: INT>,
            medium: STRUCT<url: STRING, width: INT, height: INT>,
            high: STRUCT<url: STRING, width: INT, height: INT>
        >,
        channelTitle: STRING,
        liveBroadcastContent: STRING,
        publishTime: TIMESTAMP
    >,
    data_pesquisa TIMESTAMP
)
PARTITIONED BY (ano INT, mes INT, dia INT, dia_semana string, assunto STRING)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION 'file:///datalake/bronze/assunto';

 ALTER TABLE bronze_assunto
    DROP IF EXISTS PARTITION (
        ano=2025,
        mes=4,
        dia=11,
        dia_semana='segunda',
        assunto='teste'
    )





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

drop table canais; 

create table canais(
	id_canal varchar(60),
	nome_canal string
)
PARTITIONED BY (assunto STRING)
STORED AS PARQUET;

create table videos (
	id_video varchar(80),
	nome_video string
)

PARTITIONED BY (assunto STRING)
STORED AS PARQUET;






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



