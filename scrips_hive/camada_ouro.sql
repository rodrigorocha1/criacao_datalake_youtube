-- Canais
--- Total visualizacoes_dia selecionando canal

select 
	pc.nm_canal as nome_canal,
	pc.semana as semana,
	pc.mes as mes,

	pc.total_visualizacoes as total_visualizacoes_acumuladas,
	LAG(pc.total_visualizacoes, 1, 0) OVER (PARTITION BY pc.id_canal ORDER BY pc.mes) AS visualizacoes_acumuladas_anterior,
	pc.total_visualizacoes - LAG(pc.total_visualizacoes, 1, 0) OVER (PARTITION BY pc.id_canal ORDER BY pc.mes) as TOTAL_VISUALIZACAO_DIA
from prata_canal pc 
where pc.id_canal = 'UCpcCgSD1BEDiEQ8EkY7Comw'
order by pc.mes ;


select *
from prata_canal pc 
where pc.id_canal = 'UCpcCgSD1BEDiEQ8EkY7Comw';


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
where pc.id_canal = 'UCpcCgSD1BEDiEQ8EkY7Comw'
order by pc.mes ;





--- total Inscritos dia selecionando canal

select 
	pc.nm_canal as nome_canal,
	pc.semana as semana,
	pc.mes as mes,
	pc.total_inscritos  as total_inscritos_acumuladas,
	LAG(pc.total_inscritos, 1, 0) OVER (PARTITION BY pc.id_canal ORDER BY pc.mes) AS total_inscritos_anterior,
	pc.total_inscritos - LAG(pc.total_inscritos, 1, 0) OVER (PARTITION BY pc.id_canal ORDER BY pc.mes) as total_inscritos_PUBLICADOS
from prata_canal pc 
where pc.id_canal = 'UCpcCgSD1BEDiEQ8EkY7Comw'
order by pc.mes ;



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




