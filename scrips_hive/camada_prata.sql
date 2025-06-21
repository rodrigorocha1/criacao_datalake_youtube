select *
from prata_video pv ;


select *
from prata_canal pc


SELECT
    CAST(bv.statistics.viewcount AS INT) AS total_visualizacoes,
    CAST(COALESCE(bv.statistics.likecount, 0) AS INT) AS total_likes,
    CAST(COALESCE(bv.statistics.favoritecount, 0) AS INT) AS total_favoritos,
    CAST(bv.statistics.commentcount AS INT) AS total_comentarios,
    bv.snippet.channelTitle AS nome_canal,
    bv.snippet.title AS titulo_video,
    CAST(bv.assunto AS VARCHAR(30)) AS assunto,
    CAST(bv.ano AS SMALLINT) AS ano,
    CAST(bv.mes AS TINYINT) AS mes,
    CAST(bv.dia AS TINYINT) AS dia,
    CAST(bv.dia_semana AS TINYINT) AS semana,
    bv.snippet.channelId AS id_canal,
    CONCAT_WS('-', CAST(bv.ano AS STRING), CAST(bv.mes AS STRING), CAST(bv.dia AS STRING)) AS data,
    bv.id AS id_video
FROM bronze_videos bv;


SELECT
    CAST(bv.statistics.viewcount AS INT) AS total_visualizacoes,
    CAST(COALESCE(bv.statistics.likecount, 0) AS INT) AS total_likes,
    CAST(COALESCE(bv.statistics.favoritecount, 0) AS INT) AS total_favoritos,
    CAST(bv.statistics.commentcount AS INT) AS total_comentarios,
    bv.snippet.channelTitle AS nome_canal,
    bv.snippet.title AS titulo_video,
    CAST(bv.assunto AS VARCHAR(30)) AS assunto,
    CAST(bv.ano AS SMALLINT) AS ano,
    CAST(bv.mes AS TINYINT) AS mes,
    CAST(bv.dia AS TINYINT) AS dia,
    bv.snippet.channelId AS id_canal,
    DATE_FORMAT(CONCAT_WS('-', CAST(bv.ano AS STRING), LPAD(CAST(bv.mes AS STRING), 2, '0'), LPAD(CAST(bv.dia AS STRING), 2, '0')), 'yyyy-MM-dd') AS data,
    date_format(DATE_FORMAT(CONCAT_WS('-', CAST(bv.ano AS STRING), LPAD(CAST(bv.mes AS STRING), 2, '0'), LPAD(CAST(bv.dia AS STRING), 2, '0')), 'yyyy-MM-dd'), 'EEEE'),
    CASE
        WHEN date_format(DATE_FORMAT(CONCAT_WS('-', CAST(bv.ano AS STRING), LPAD(CAST(bv.mes AS STRING), 2, '0'), LPAD(CAST(bv.dia AS STRING), 2, '0')), 'yyyy-MM-dd'), 'EEEE') = 'Sunday' THEN 'Domingo'
        WHEN date_format(DATE_FORMAT(CONCAT_WS('-', CAST(bv.ano AS STRING), LPAD(CAST(bv.mes AS STRING), 2, '0'), LPAD(CAST(bv.dia AS STRING), 2, '0')), 'yyyy-MM-dd'), 'EEEE') = 'Monday' THEN 'Segunda-feira'
        WHEN date_format(DATE_FORMAT(CONCAT_WS('-', CAST(bv.ano AS STRING), LPAD(CAST(bv.mes AS STRING), 2, '0'), LPAD(CAST(bv.dia AS STRING), 2, '0')), 'yyyy-MM-dd'), 'EEEE') = 'Tuesday' THEN 'Terça-feira'
        WHEN date_format(DATE_FORMAT(CONCAT_WS('-', CAST(bv.ano AS STRING), LPAD(CAST(bv.mes AS STRING), 2, '0'), LPAD(CAST(bv.dia AS STRING), 2, '0')), 'yyyy-MM-dd'), 'EEEE') = 'Wednesday' THEN 'Quarta-feira'
        WHEN date_format(DATE_FORMAT(CONCAT_WS('-', CAST(bv.ano AS STRING), LPAD(CAST(bv.mes AS STRING), 2, '0'), LPAD(CAST(bv.dia AS STRING), 2, '0')), 'yyyy-MM-dd'), 'EEEE') = 'Thursday' THEN 'Quinta-feira'
        WHEN date_format(DATE_FORMAT(CONCAT_WS('-', CAST(bv.ano AS STRING), LPAD(CAST(bv.mes AS STRING), 2, '0'), LPAD(CAST(bv.dia AS STRING), 2, '0')), 'yyyy-MM-dd'), 'EEEE') = 'Friday' THEN 'Sexta-feira'
        WHEN date_format(DATE_FORMAT(CONCAT_WS('-', CAST(bv.ano AS STRING), LPAD(CAST(bv.mes AS STRING), 2, '0'), LPAD(CAST(bv.dia AS STRING), 2, '0')), 'yyyy-MM-dd'), 'EEEE') = 'Saturday' THEN 'Sábado'
        ELSE 'Desconhecido'
    END AS dia_semana_pt,
    bv.id AS id_video
FROM bronze_videos bv;
