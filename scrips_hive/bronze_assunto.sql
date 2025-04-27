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
from bronze_assunto
order by assunto desc
select 1;


select *
from canais;

SELECT 1
            FROM videos 
            WHERE id_video = 'a'
            LIMIT 1   

SELECT 1
            FROM canais 
            WHERE id_canal = 'UCRvcwdGvUUYkDkUvqj4UYjA'
            LIMIT 1  

INSERT INTO videos 
                    PARTITION (assunto="No Man's Sky")
                    VALUES ('hFmcrZ8zoqc', 'No Man&#39;s Sky [PS4] | A culpa é minha...a vida poderia sim, ser mais bela...saber que...eu..s.. |')


INSERT INTO videos 
                    PARTITION (assunto='No Man's Sky')
                    VALUES ('hFmcrZ8zoqc', 'No Man&#39;s Sky [PS4] | A culpa é minha...a vida poderia sim, ser mais bela...saber que...eu..s.. |')
drop table bronze_assunto

DELETE FROM youtube.bronze_assunto
WHERE kind='' AND etag='' AND id=? AND snippet=? AND data_pesquisa='' AND ano=0 AND mes=0 AND dia=0 AND dia_semana='' AND assunto='';


