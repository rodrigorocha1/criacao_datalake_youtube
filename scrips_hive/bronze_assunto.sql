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


DROP TABLE bronze_assunto;

     ALTER TABLE bronze_assunto
            ADD IF NOT EXISTS PARTITION (
            ano=2025,
            mes=4,
            dia=26,
            dia_semana="Sábado",
            assunto="Python"
        )
        
ALTER TABLE bronze_assunto
ADD IF NOT EXISTS PARTITION (
    ano=2025,
    mes=4,
    dia=26,
    dia_semana=CONVERT('Sábado' USING utf8mb4),
    assunto="Python"
);


SHOW VARIABLES LIKE 'character_set%';



