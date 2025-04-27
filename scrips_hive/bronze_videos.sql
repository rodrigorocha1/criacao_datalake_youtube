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
    >
)
PARTITIONED BY (ano INT, mes INT, dia INT, dia_semana STRING, assunto STRING)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION 'file:///home/hadoop/datalake/bronze/videos';


select *
from bronze_assunto ba ;


select *
from bronze_canais bc ;

select *
from bronze_videos bv ;

select bv.snippet.channelid,
	bv.sni
	
from bronze_videos bv 


select *
from videos v ;

select *
from canais c ;

show partitions bronze_assunto;

ALTER TABLE bronze_assunto DROP PARTITION (ano='2025', mes='4', dia='23', dia_semana='Quarta-feira', assunto='Danilo');
ALTER TABLE bronze_assunto DROP PARTITION (ano='2025', mes='4', dia='26', dia_semana='Sábado', assunto='Cities Skylines');
ALTER TABLE bronze_assunto DROP PARTITION (ano='2025', mes='4', dia='26', dia_semana='Sábado', assunto='No Man''s Sky');
ALTER TABLE bronze_assunto DROP PARTITION (ano='2025', mes='4', dia='26', dia_semana='Sábado', assunto='Python');

     ALTER TABLE bronze_assunto
            ADD IF NOT EXISTS PARTITION (
            ano=2025,
            mes=4,
            dia=26,
            dia_semana='Sábado',
            assunto="Python"
        )
        



ALTER TABLE bronze_assunto  DROP IF EXISTS PARTITION (coluna_particao='valor_particao');



















