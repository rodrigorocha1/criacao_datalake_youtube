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
            dia_semana='Saábado',
            assunto='Python'
        )
        
  show partitions   bronze_assunto 
        
ALTER TABLE bronze_assunto
DROP IF EXISTS PARTITION (
    ano=2026,
    mes=4,
    dia=26,
    dia_semana='ábado',
    assunto='Píthon'
);        
        
ALTER TABLE bronze_assunto
ADD IF NOT EXISTS PARTITION (
    ano=2026,
    mes=4,
    dia=26,
    dia_semana='Sábado',
    assunto="Píthon"
);

ALTER TABLE bronze_assunto ADD PARTITION (dia='Sábado');

SHOW VARIABLES LIKE 'character_set%';

select *
from videos v2  


 SELECT DISTINCT c.id_canal
            FROM canais c 
            where c.assunto = "No_Mans_Sky"

select *  
from bronze_assunto
where dia = 10
select 1;

select *
from 


INSERT INTO videos 
                    PARTITION (assunto="No_Mans_Sky")
                    VALUES ('NlRHLtDjgqU', '👨\u200d🚀 Continuando o dilema do ATLAS no No Man&#39;s Sky VR!🚀 !discord !donate !exitlag !amazon')
select *
from canais;

SELECT 1
            FROM videos 
            WHERE id_video = 'a'
            LIMIT 1   

      ALTER TABLE bronze_assunto
                    ADD IF NOT EXISTS PARTITION ( 
                        ano=2025,
                        mes=4,
                        dia='Domingo',
                        dia_semana='Domingo',
                        assunto="No_Mans_Sky"
                )
                
                
  select * 
  from bronze_assunto ba;
  
  
drop table bronze_assunto ;
  
select *
from bronze_assunto ba
where ba.data_pesquisa is not null;
  
ALTER TABLE bronze_assunto
                        ADD IF NOT EXISTS PARTITION ( 
                            ano=2025,
                            mes=5,
                            dia=4,
                            dia_semana='Domingo',
                            assunto="No_Mans_Sky"
                    )
                    
                    