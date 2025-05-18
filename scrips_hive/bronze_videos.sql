CREATE EXTERNAL TABLE bronze_videos(
    kind STRING,
    etag STRING,
    id STRING,
    snippet STRUCT<
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
    contentDetails STRUCT<
        duration: STRING,
        dimension: STRING,
        definition: STRING,
        caption: STRING,
        licensedContent: BOOLEAN,
        contentRating: MAP<STRING, STRING>,
        projection: STRING
    >,
    status STRUCT<
        uploadStatus: STRING,
        privacyStatus: STRING,
        license: STRING,
        embeddable: BOOLEAN,
        publicStatsViewable: BOOLEAN,
        madeForKids: BOOLEAN
    >,
    statistics STRUCT<
        viewCount: BIGINT,
        likeCount: BIGINT,
        favoriteCount: BIGINT,
        commentCount: BIGINT
    >,
    pageInfo STRUCT<
        totalResults: INT,
        resultsPerPage: INT
    >,
    data_pesquisa TIMESTAMP
)
PARTITIONED BY (ano INT, mes INT, dia INT, dia_semana STRING, assunto STRING)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION 'file:///opt/hive/datalake/bronze/videos';


create external table videos (
	id_video VARCHAR(80),
	nome_video string
	
) partitioned by (assunto STRING)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION 'file:///home/hadoop/datalake/depara/videos';




SELECT 1 FROM youtube.videos WHERE id_video = 'lBCbqciE26w' AND assunto = "No_Mans_Sky" LIMIT 1

drop table bronze_videos;

select v.id
from bronze_videos v ;

select *
from bronze_assunto ba 


drop table bronze_videos;

alter table bronze_videos 
add if not exists partition (
	ano=2025,
	mes=4,
	dia=27,
	dia_semana="Domingo",
	assunto="No_Mans_Sky"
)


select *
from bronze_canais bc ;



SELECT *
FROM temp_canal_video



CREATE EXTERNAL TABLE `youtube`.`temp_canal_video`(
  `id_canal` string COMMENT 'from deserializer', 
  `id_video` string COMMENT 'from deserializer',
  `assunto` string COMMENT 'from deserializer')
ROW FORMAT SERDE 
  'org.apache.hive.hcatalog.data.JsonSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'file:/opt/hive/datalake/temp'
TBLPROPERTIES (
  'bucketing_version'='2', 
  'transient_lastDdlTime'='1747001512');




