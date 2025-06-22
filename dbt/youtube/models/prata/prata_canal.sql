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
        CASE
            WHEN date_format(DATE_FORMAT(CONCAT_WS('-', CAST(bc.ano AS STRING), LPAD(CAST(bc.mes AS STRING), 2, '0'), LPAD(CAST(bc.dia AS STRING), 2, '0')), 'yyyy-MM-dd'), 'EEEE') = 'Sunday' THEN 'Domingo'
            WHEN date_format(DATE_FORMAT(CONCAT_WS('-', CAST(bc.ano AS STRING), LPAD(CAST(bc.mes AS STRING), 2, '0'), LPAD(CAST(bc.dia AS STRING), 2, '0')), 'yyyy-MM-dd'), 'EEEE') = 'Monday' THEN 'Segunda-feira'
            WHEN date_format(DATE_FORMAT(CONCAT_WS('-', CAST(bc.ano AS STRING), LPAD(CAST(bc.mes AS STRING), 2, '0'), LPAD(CAST(bc.dia AS STRING), 2, '0')), 'yyyy-MM-dd'), 'EEEE') = 'Tuesday' THEN 'Terça-feira'
            WHEN date_format(DATE_FORMAT(CONCAT_WS('-', CAST(bc.ano AS STRING), LPAD(CAST(bc.mes AS STRING), 2, '0'), LPAD(CAST(bc.dia AS STRING), 2, '0')), 'yyyy-MM-dd'), 'EEEE') = 'Wednesday' THEN 'Quarta-feira'
            WHEN date_format(DATE_FORMAT(CONCAT_WS('-', CAST(bc.ano AS STRING), LPAD(CAST(bc.mes AS STRING), 2, '0'), LPAD(CAST(bc.dia AS STRING), 2, '0')), 'yyyy-MM-dd'), 'EEEE') = 'Thursday' THEN 'Quinta-feira'
            WHEN date_format(DATE_FORMAT(CONCAT_WS('-', CAST(bc.ano AS STRING), LPAD(CAST(bc.mes AS STRING), 2, '0'), LPAD(CAST(bc.dia AS STRING), 2, '0')), 'yyyy-MM-dd'), 'EEEE') = 'Friday' THEN 'Sexta-feira'
            WHEN date_format(DATE_FORMAT(CONCAT_WS('-', CAST(bc.ano AS STRING), LPAD(CAST(bc.mes AS STRING), 2, '0'), LPAD(CAST(bc.dia AS STRING), 2, '0')), 'yyyy-MM-dd'), 'EEEE') = 'Saturday' THEN 'Sábado'
        ELSE 'Desconhecido' END  as semana,
        CAST(bc.id AS STRING) as id_canal
    from {{ source('camada_bronze', 'bronze_canais') }} bc
    {% if is_incremental() %}
    where bc.dia = {{ dia }} and bc.mes = {{ mes }} and bc.ano = {{ ano }}
    {% endif %}


)



select * from source_data