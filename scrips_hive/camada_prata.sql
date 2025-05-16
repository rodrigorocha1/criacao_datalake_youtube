select 

	bc.statistics.viewcount as total_visualizacoes, 
	bc.statistics.videocount as total_videos_publicados,
	bc.statistics.subscribercount as total_inscritos,
	bc.assunto as assunto,
	bc.ano as ano,
	bc.mes as mes,
	bc.dia as dia,
	bc.dia_semana as semana,
 	bc.id as id_canal,
 	bc.snippet.title as nm_canal
from bronze_canais bc 
where bc.ano = year(current_date)
and bc.mes = MONTH(CURRENT_date)
and bc.dia = day(current_date);

SELECT current_date, year(current_date), MONTH(CURRENT_date), day(current_date);


select *
from prata_canal pc ;

select 

	DISTINCT
 	bc.id as id_canal

from bronze_canais bc  ;

select *
from my_second_dbt_model msdm   

select 

	bv.statistics.viewcount as total_visualizacoes,
	coalesce(bv.statistics.likecount, 0) as total_likes,
	coalesce(bv.statistics.favoritecount, 0)  total_favoritos,
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
from bronze_videos bv ;

select *
from bronze_videos bv 













select bc.statistics.viewcount as total_visualizacoes, bc.statistics.videocount as total_videos_publicados, bc.statistics.subscribercount as total_inscritos, bc.snippet.title as nm_canal, bc.assunto as assunto, bc.ano as ano, bc.mes as mes, bc.dia as dia, bc.dia_semana as semana, bc.id as id_canal from youtube.bronze_canais bc where bc.ano = 2025 and bc.mes = 5 and bc.dia = 15 











