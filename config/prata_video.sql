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
        bv.statistics.viewcount as total_visualizacoes,
        coalesce(bv.statistics.likecount, 0) as total_likes,
        coalesce(bv.statistics.favoritecount, 0) as total_favoritos,
        bv.statistics.commentcount as total_comentarios,
        bv.snippet.channelTitle as nome_canal,
        bv.snippet.title as titulo_video,
        bv.assunto as assunto,
        bv.ano as ano,
        bv.mes as mes,
        bv.dia as dia,
        bv.dia_semana as semana,
        bv.snippet.channelId as id_canal,
        bv.id as id_video
    from {{ source('camada_bronze', 'bronze_videos') }} bv
    {% if is_incremental() %}
    where bv.dia = {{ dia }} and bv.mes = {{ mes }} and bv.ano = {{ ano }}
    {% endif %}
)

select * FROM prata_video