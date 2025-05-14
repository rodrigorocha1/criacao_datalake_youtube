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

drop table bronze_canais;

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
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION 'file:///home/hadoop/datalake/depara/canais';


 ALTER TABLE canais
                ADD IF NOT EXISTS PARTITION (
                    assunto="a"
                )

                
alter table CANAIS 
drop partition (assunto="No_Mans_Sky")

select distinct id
from bronze_canais bc ;

drop table canais

ALTER TABLE bronze_canais ADD COLUMNS (data_pesquisa STRING);
SELECT DISTINCT assunto FROM youtube.bronze_canais;

	select distinct bc.id from youtube.bronze_canais bc where bc.assunto = 'no_mans_sky'
select distinct ID_CANAL
from temp_canal_video

set mapreduce.map.memory.mb=2048;
                set mapreduce.reduce.memory.mb=2048;
                set mapreduce.map.java.opts=-Xmx1536m;
                set mapreduce.reduce.java.opts=-Xmx1536m;
            
            select distinct ID_CANAL from youtube.temp_canal_video;



select  ID_CANAL, ID_VIDEO 
from youtube.temp_canal_video
limit 100;

select distinct bc.id from youtube.bronze_canais bc where bc.assunto = 'no_mans_sky'

describe temp_canal_video;

drop table bronze_canais ;
create external table temp_canal_video(
	ID_CANAL string,
	ID_VIDEO string
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION 'file:///home/hadoop/datalake/temp';


SELECT DISTINCT bc.id 
FROM youtube.bronze_canais bc 
WHERE bc.assunto = 'no_mans_sky'
LIMIT 10;

select *
from bronze_canais

select  bc.id
            from youtube.bronze_canais bc  
            where bc.assunto = 'python'


select bc.id from youtube.bronze_canais bc where bc.assunto = 'cities_skylines'


select  *
from temp_canal_video tcv 


select  *
            from youtube.bronze_canais bc  
            where bc.assunto = 'python'



















            select ID_CANAL from youtube.temp_canal_video where assunto = 'palworld'









