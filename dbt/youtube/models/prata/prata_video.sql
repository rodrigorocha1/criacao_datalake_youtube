{{
    config(
        materialized='incremental',
        partition_by=['assunto', 'ano', 'mes', 'dia', 'semana', 'id_canal', 'id_video'],
        file_format='PARQUET'
    )
}}

{% set today = modules.datetime.date.today() %}
{% set ano = today.year %}
{% set mes = today.month %}
{% set dia = today.day %}

with prata_video as (
    select
        cast(bv.statistics.viewcount as int) as total_visualizacoes,
        cast(coalesce(bv.statistics.likecount, 0) as int) as total_likes,
        cast(coalesce(bv.statistics.favoritecount, 0) as int) as total_favoritos,
        cast(bv.statistics.commentcount as INT) as total_comentarios,
        bv.snippet.channelTitle as nome_canal,
        bv.snippet.title as titulo_video,
        cast(bv.assunto as VARCHAR(30)) as assunto,
        cast(bv.ano as SMALLINT) as ano,
        cast(bv.mes as TINYINT) as mes,
        cast(bv.dia as TINYINT) as dia,
        CASE
            WHEN date_format(DATE_FORMAT(CONCAT_WS('-', CAST(bv.ano AS STRING), LPAD(CAST(bv.mes AS STRING), 2, '0'), LPAD(CAST(bv.dia AS STRING), 2, '0')), 'yyyy-MM-dd'), 'EEEE') = 'Sunday' THEN 'Domingo'
            WHEN date_format(DATE_FORMAT(CONCAT_WS('-', CAST(bv.ano AS STRING), LPAD(CAST(bv.mes AS STRING), 2, '0'), LPAD(CAST(bv.dia AS STRING), 2, '0')), 'yyyy-MM-dd'), 'EEEE') = 'Monday' THEN 'Segunda-feira'
            WHEN date_format(DATE_FORMAT(CONCAT_WS('-', CAST(bv.ano AS STRING), LPAD(CAST(bv.mes AS STRING), 2, '0'), LPAD(CAST(bv.dia AS STRING), 2, '0')), 'yyyy-MM-dd'), 'EEEE') = 'Tuesday' THEN 'Terça-feira'
            WHEN date_format(DATE_FORMAT(CONCAT_WS('-', CAST(bv.ano AS STRING), LPAD(CAST(bv.mes AS STRING), 2, '0'), LPAD(CAST(bv.dia AS STRING), 2, '0')), 'yyyy-MM-dd'), 'EEEE') = 'Wednesday' THEN 'Quarta-feira'
            WHEN date_format(DATE_FORMAT(CONCAT_WS('-', CAST(bv.ano AS STRING), LPAD(CAST(bv.mes AS STRING), 2, '0'), LPAD(CAST(bv.dia AS STRING), 2, '0')), 'yyyy-MM-dd'), 'EEEE') = 'Thursday' THEN 'Quinta-feira'
            WHEN date_format(DATE_FORMAT(CONCAT_WS('-', CAST(bv.ano AS STRING), LPAD(CAST(bv.mes AS STRING), 2, '0'), LPAD(CAST(bv.dia AS STRING), 2, '0')), 'yyyy-MM-dd'), 'EEEE') = 'Friday' THEN 'Sexta-feira'
            WHEN date_format(DATE_FORMAT(CONCAT_WS('-', CAST(bv.ano AS STRING), LPAD(CAST(bv.mes AS STRING), 2, '0'), LPAD(CAST(bv.dia AS STRING), 2, '0')), 'yyyy-MM-dd'), 'EEEE') = 'Saturday' THEN 'Sábado'
            ELSE 'Desconhecido'
        END AS semana,
        bv.snippet.channelId as id_canal,
        bv.id as id_video
    from {{ source('camada_bronze', 'bronze_videos') }} bv
   {% if is_incremental() %}
   where bv.dia = {{ dia }} and bv.mes = {{ mes }} and bv.ano = {{ ano }}
   {% endif %}
)

select * FROM prata_video