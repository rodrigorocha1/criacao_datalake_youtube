{{
    config(
        materialized='incremental',
        partition_by=['assunto', 'ano', 'mes', 'dia', 'indice_semana', 'id_canal'],
        file_format='PARQUET'
    )
}}


{% set today = modules.datetime.date.today() %}
{% set ano = today.year %}
{% set mes = today.month %}
{% set dia = today.day %}


with ouro_canal as (
    select
        pc.total_visualizacoes as total_visualizacoes_acumuladas,
        LAG(pc.total_visualizacoes, 1, 0) OVER (PARTITION BY pc.id_canal ORDER BY pc.mes, pc.dia ) AS visualizacoes_acumuladas_anterior,
        pc.total_visualizacoes - LAG(pc.total_visualizacoes, 1, 0) OVER (PARTITION BY pc.id_canal ORDER BY pc.mes, pc.dia) as TOTAL_VISUALIZACAO_DIA,
        pc.nm_canal as nome_canal,
        DATE_FORMAT(CONCAT_WS('-', CAST(pc.ano AS STRING), LPAD(CAST(pc.mes AS STRING), 2, '0'), LPAD(CAST(pc.dia AS STRING), 2, '0')), 'yyyy-MM-dd') dia_formatado,
        CAST(pc.semana as varchar(30)) as semana,
        pc.assunto as assunto,
        cast(pc.ano as SMALLINT) as ano,
        cast(pc.mes as TINYINT) as mes,
        cast(pc.dia as TINYINT) as dia,
        case when pc.semana == 'Domingo'
            then 0
        when pc.semana == 'Segunda'
            then 1
        when pc.semana == 'Terça'
            then 2
        when pc.semana == 'Quarta'
            then 3
        when pc.semana == 'Quinta'
            then 4
        when pc.semana == 'Sexta'
            then 5
        when pc.semana == 'Sábado'
            then 6 end as indice_semana,
        pc.id_canal as id_canal
    from   {{ source('camada_ouro', 'prata_canal') }} pc
    WHERE
        DATE_FORMAT(CONCAT_WS('-', CAST(pc.ano AS STRING), LPAD(CAST(pc.mes AS STRING), 2, '0'), LPAD(CAST(pc.dia AS STRING), 2, '0')), 'yyyy-MM-dd') >= CURRENT_DATE() - 1
        {% if is_incremental() %}

        AND DATE_FORMAT(CONCAT_WS('-', CAST(pc.ano AS STRING), LPAD(CAST(pc.mes AS STRING), 2, '0'), LPAD(CAST(pc.dia AS STRING), 2, '0')), 'yyyy-MM-dd') > (SELECT MAX(dia_formatado) FROM {{ this }})
    {% endif %}

)

select * FROM ouro_canal