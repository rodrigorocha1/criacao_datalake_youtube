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
        CAST(bc.statistics.viewcount as int) as total_visualizacoes,
        CAST(bc.statistics.videocount as int) as total_videos_publicados,
        CAST(bc.statistics.subscribercount as int) as total_inscritos,
        CAST(bc.snippet.title as STRING) as nm_canal,
        CAST(bc.assunto as varchar(30)) as assunto,
        CAST(bc.ano as SMALLINT) as ano,
        CAST( bc.mes AS TINYINT) as mes,
        CAST(bc.dia as TINYINT) as dia,
        CAST(bc.dia_semana as VARCHAR(30)) as semana,
        CAST(bc.id AS STRING) as id_canal
    from {{ source('camada_bronze', 'bronze_canais') }} bc
    {% if is_incremental() %}
    where bc.dia = {{ dia }} and bc.mes = {{ mes }} and bc.ano = {{ ano }}
    {% endif %}


)



select * from source_data