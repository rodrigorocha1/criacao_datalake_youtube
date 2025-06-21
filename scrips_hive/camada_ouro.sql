-- Canais
--- Total visualizacoes_dia selecionando canal

select 
	pc.total_visualizacoes as total_visualizacoes_acumuladas,
	LAG(pc.total_visualizacoes, 1, 0) OVER (PARTITION BY pc.id_canal ORDER BY pc.mes, pc.dia ) AS visualizacoes_acumuladas_anterior,
	pc.total_visualizacoes - LAG(pc.total_visualizacoes, 1, 0) OVER (PARTITION BY pc.id_canal ORDER BY pc.mes, pc.dia) as TOTAL_VISUALIZACAO_DIA,
	pc.nm_canal as nome_canal,
	DATE_FORMAT(CONCAT_WS('-', CAST(pc.ano AS STRING), LPAD(CAST(pc.mes AS STRING), 2, '0'), LPAD(CAST(pc.dia AS STRING), 2, '0')), 'yyyy-MM-dd') dia_formatado,
	pc.semana as semana,
	pc.assunto as assunto,
	pc.ano as ano,
	pc.mes as mes,
	pc.dia as dia,
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
from prata_canal pc 
 -- where pc.id_canal = 'UCGxmd2AnLNrSWFhrCcEj0lQ'  
-- AND  pc.dia BETWEEN 24 and 25  and pc.mes in (5, 6)
 where DATE_FORMAT(CONCAT_WS('-', CAST(pc.ano AS STRING), LPAD(CAST(pc.mes AS STRING), 2, '0'), LPAD(CAST(pc.dia AS STRING), 2, '0')), 'yyyy-MM-dd') >= CURRENT_DATE() - 1
order by pc.id_canal , pc.mes, pc.dia ;


SELECT CURRENT_DATE() - 1;


select *
from prata_canal pc 
where pc.id_canal = 'UCGxmd2AnLNrSWFhrCcEj0lQ';


SELECT
  total_visualizacoes,
  total_videos_publicados,
  total_inscritos,
  nm_canal,
  (CAST(total_visualizacoes AS DECIMAL) / total_inscritos) * 100 AS taxa_engajamento_percentual
FROM
  prata_canal
WHERE
  total_inscritos > 0; -- Para evitar divisão por zero


-----Média de Visualizações por Vídeo total_visualizacoes / total_videos_publicados
  
  
----- Engajamento por Vídeo Publicado: (total_visualizacoes / total_videos_publicados) / total_inscritos


--- Total Vídeos Públicados dia selecionando canal

select 
	pc.nm_canal as nome_canal,
	pc.semana as semana,
	pc.mes as mes,
	pc.total_videos_publicados as total_videos_publicados_acumuladas,
	LAG(pc.total_videos_publicados, 1, 0) OVER (PARTITION BY pc.id_canal ORDER BY pc.mes) AS videos_acumuladas_anterior,
	pc.total_videos_publicados - LAG(pc.total_videos_publicados, 1, 0) OVER (PARTITION BY pc.id_canal ORDER BY pc.mes) as TOTAL_VIDEOS_PUBLICADOS
from prata_canal pc 
-- where pc.id_canal = 'UCpcCgSD1BEDiEQ8EkY7Comw'
order by pc.id_canal ,pc.mes ;





--- total Inscritos dia selecionando canal

select 
	pc.nm_canal as nome_canal,
	pc.semana as semana,
	pc.mes as mes,
	pc.total_inscritos  as total_inscritos_acumuladas,
	LAG(pc.total_inscritos, 1, 0) OVER (PARTITION BY pc.id_canal ORDER BY pc.mes) AS total_inscritos_anterior,
	pc.total_inscritos - LAG(pc.total_inscritos, 1, 0) OVER (PARTITION BY pc.id_canal ORDER BY pc.mes) as total_inscritos_PUBLICADOS
from prata_canal pc 
--- where pc.id_canal = 'UCpcCgSD1BEDiEQ8EkY7Comw'
order by pc.id_canal ,pc.mes ;



--- Videos
--- Total Visualizacoes dia


select pv.ano,
	pv.mes,
	pv.dia,

	pv.id_canal,
	pv.id_video,
	pv.total_comentarios,
	pv.total_favoritos,
	pv.total_likes,
	pv.total_visualizacoes
from prata_video pv 
order by id_video


select *
from bronze_canais bc 


show partitions bronze_videos


ALTER TABLE bronze_videos  DROP IF EXISTS PARTITION (ano=2025, mes=5, dia=24, dia_semana='Sabado', assunto='no_mans_sky');
ALTER TABLE bronze_videos DROP IF EXISTS PARTITION (ano=2025, mes=5, dia=24, dia_semana='Sabado', assunto='palworld');
ALTER TABLE bronze_videos DROP IF EXISTS PARTITION (ano=2025, mes=5, dia=24, dia_semana='Sabado', assunto='python');
ALTER TABLE bronze_videos DROP IF EXISTS PARTITION (ano=2025, mes=5, dia=25, dia_semana='Domingo', assunto='no_mans_sky');
ALTER TABLE bronze_videos DROP IF EXISTS PARTITION (ano=2025, mes=5, dia=25, dia_semana='Domingo', assunto='palworld');
ALTER TABLE bronze_videos DROP IF EXISTS PARTITION (ano=2025, mes=5, dia=25, dia_semana='Domingo', assunto='python');
ALTER TABLE bronze_videos DROP IF EXISTS PARTITION (ano=2025, mes=6, dia=1, dia_semana='Domingo', assunto='no_mans_sky');
ALTER TABLE bronze_videos DROP IF EXISTS PARTITION (ano=2025, mes=6, dia=1, dia_semana='Domingo', assunto='palworld');
ALTER TABLE bronze_videos DROP IF EXISTS PARTITION (ano=2025, mes=6, dia=1, dia_semana='Domingo', assunto='python');


