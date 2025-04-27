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
        hiddenSubscrfile:/home/rodrigo/Documentos/projetos/criacao_datalake_youtube/scrips_hive/bronze_assunto.sqliberCount: BOOLEAN,
        videoCount: STRING
    >
)
PARTITIONED BY (ano INT, mes INT, dia INT, dia_semana STRING, assunto STRING)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION 'file:///home/hadoop/datalake/bronze/canais';


select *
from bronze_canais bc 