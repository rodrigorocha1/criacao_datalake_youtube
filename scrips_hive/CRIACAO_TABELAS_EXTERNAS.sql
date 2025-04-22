-- Bronze ---------------------------------------------------

select * 
from canais c ;

TRUNCATE  table canais;

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

show partitions bronze_canais;

drop table bronze_assunto

select *
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
LOCATION 'file:///home/hadoop/datalake/bronze/assunto';

select  1

select *
from bronze_canais;


         ALTER TABLE bronze_canais
                        ADD  PARTITION (
                            ano=2025,
                            mes=4,
                            dia=22,
                            dia_semana='Segunda-feira',
                            assunto="Danilo"
                        )

drop table bronze_canais;


CREATE EXTERNAL TABLE bronze_canais (
    kind STRING,
    etag STRING,
    id STRING,
    snippet STRUCT<
        title: STRING,
        description: STRING,
        customUrl: STRING,
        publishedAt: STRING,
        thumbnails: MAP<STRING, STRUCT<url: STRING, width: INT, height: INT>>,
        defaultLanguage: STRING,
        localized: STRUCT<title: STRING, description: STRING>,
        country: STRING
    >,
    statistics STRUCT<
        viewCount: STRING,
        subscriberCount: STRING,
        hiddenSubscriberCount: BOOLEAN,
        videoCount: STRING
    >
)
PARTITIONED BY (ano INT, mes INT, dia INT, dia_semana STRING, assunto STRING)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION 'file:///home/hadoop/datalake/bronze/canais';

drop table bronze_canais;

 ALTER TABLE bronze_canais
    DROP IF EXISTS PARTITION (
        ano=2025,
        mes=4,
        dia=21,
        dia_semana='Segunda-feira',
        assunto='Danilo'
    )
select *
from bronze_canais;

create external table bronze_canais  (
    kind STRING,
    etag STRING,
    id STRING,
    snippet STRUCT<
        title: STRING,
        description: STRING,
        customUrl: STRING,
        publishedAt: STRING,
        thumbnails: MAP<STRING, STRUCT<url: STRING, width: INT, height: INT>>,
        defaultLanguage: STRING,
        localized: STRUCT<title: STRING, description: STRING>,
        country: STRING
    >,
    statistics STRUCT<
        viewCount: STRING,
        subscriberCount: STRING,
        hiddenSubscriberCount: BOOLEAN,
        videoCount: STRING
    >
)
PARTITIONED BY (ano INT, mes INT, dia INT, dia_semana string, assunto STRING)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION 'file:///home/hadoop/datalake/bronze/canais';

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


select *




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



