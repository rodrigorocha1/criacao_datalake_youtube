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
from bronze_canais bc  ;


select 

	bv.statistics.viewcount as total_visualizacoes,
	bv.statistics.likecount as total_likes,
	bv.statistics.favoritecount  total_favoritos,
	bv.statistics.commentcount as total_comentarios,
	bv.assunto as assunto,
	bv.ano as ano,
	bv.mes as mes,
	bv.dia as dia,
	bv.dia_semana as semana,
  bv.snippet.channelId as id_canal,
  bv.snippet.channelTitle as nome_canal,
  bv.id as id_video,
  bv.snippet.title as titulo_video
	
from bronze_videos bv ;

select *
from bronze_videos;


SELECT date_format(current_timestamp, 'yyyy-MM-dd HH:mm:ss') AS formatted_time;

select *
from teste_particao_external_ttable tpet ;

select *
from tteste_particao_external_ttable tpet ;

select *
from clientes;

select *
from produtos;

select *
from stg_usuarios su  ;


create database teste;


























