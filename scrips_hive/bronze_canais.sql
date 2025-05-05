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
        hiddenSubscrfile: BOOLEAN,
        videoCount: STRING
    >,
    data_pesquisa string 
)
PARTITIONED BY (ano INT, mes INT, dia INT, dia_semana STRING, assunto STRING)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION 'file:///home/hadoop/datalake/bronze/canais';

alter table bronze_canais 
add if not exists partition (
	ano=2025,
	mes=4,
	dia=27,
	dia_semana="Domingo",
	assunto="No_Mans_Sky"
)


create external table canais (
	id_canal VARCHAR(80),
	nome_canal STRING
) partitioned by (assunto STRING)
STORED AS TEXTFILE
LOCATION 'file:///home/hadoop/datalake/depara/canais';


 ALTER TABLE canais
                ADD IF NOT EXISTS PARTITION (
                    assunto="a"
                )

                
alter table CANAIS 
drop partition (assunto="No_Mans_Sky")

select *
from bronze_canais

drop table canais

ALTER TABLE bronze_canais ADD COLUMNS (data_pesquisa STRING);
