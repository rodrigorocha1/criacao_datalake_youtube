-- Bronze ---------------------------------------------------
create external table if not exists bronze_assunto(
	json_assunto STRING
) row format serde 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED as TEXTFILE
location '/datalake/bronze/assunto';



create external table if not exists bronze_canais(
	json_canais STRING
) row format serde 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED as TEXTFILE
location 'file:///datalake/bronze/canais';


create external table if not exists bronze_videos(
	json_videos STRING
) row format serde 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED as TEXTFILE
location 'file:///datalake/bronze/videos';


--------------------------------------------------------------
CREATE TABLE teste (
    id INT,
    name STRING
)
PARTITIONED BY (city STRING)
STORED AS PARQUET


CREATE database youtube;

SHOW TABLES;


DESCRIBE FORMATTED teste_particao_external_ttable;


