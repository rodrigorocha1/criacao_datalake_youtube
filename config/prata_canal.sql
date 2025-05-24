{{
    config(
        materialized='incremental',
        partition_by=['assunto', 'ano', 'mes', 'dia', 'semana', 'id_canal'],
        file_format='PARQUET'
    )
}}


{% set today = modules.datetime.date.today() %}
{% set ano = today.year %}
{% set mes = today.month %}
{% set dia = today.day %}



with source_data as (
     select
        bc.statistics.viewcount as total_visualizacoes,
        bc.statistics.videocount as total_videos_publicados,
        bc.statistics.subscribercount as total_inscritos,
        bc.snippet.title as nm_canal,
        bc.assunto as assunto,
        bc.ano as ano,
        bc.mes as mes,
        bc.dia as dia,
        bc.dia_semana as semana,
        bc.id as id_canal
    from {{ source('camada_bronze', 'bronze_canais') }} bc
    {% if is_incremental() %}
    where bc.dia = {{ dia }} and bc.mes = {{ mes }} and bc.ano = {{ ano }}
    {% endif %}


)



select * from source_data