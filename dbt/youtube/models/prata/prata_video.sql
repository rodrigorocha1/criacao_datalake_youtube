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
        cast(bv.dia_semana as TINYINT) as semana,
        bv.snippet.channelId as id_canal,
        bv.id as id_video
    from {{ source('camada_bronze', 'bronze_videos') }} bv
   -- {% if is_incremental() %}
   -- where bv.dia = {{ dia }} and bv.mes = {{ mes }} and bv.ano = {{ ano }}
    -- {% endif %}
)

select * FROM prata_video