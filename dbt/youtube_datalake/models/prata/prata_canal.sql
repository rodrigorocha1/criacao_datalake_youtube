{{
    config(
        materialized='table',
        partition_by=['assunto', 'ano', 'mes', 'dia', 'semana', 'id_canal'],
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
    from youtube.bronze_canais bc


)



select * from source_data