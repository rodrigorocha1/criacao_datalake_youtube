select 

	bc.statistics.viewcount as total_visualizacoes, 
	bc.statistics.videocount as total_videos_publicados,
	bc.statistics.subscribercount as total_inscritos,
	year(bc.data_pesquisa) as ano,
	month(bc.data_pesquisa) as mes,
	bc.data_pesquisa as data,
	CASE dayofweek(bc.data_pesquisa)
	    WHEN 1 THEN 'Domingo'
	    WHEN 2 THEN 'Segunda-feira'
	    WHEN 3 THEN 'Terça-feira'
	    WHEN 4 THEN 'Quarta-feira'
	    WHEN 5 THEN 'Quinta-feira'
	    WHEN 6 THEN 'Sexta-feira'
	    WHEN 7 THEN 'Sábado'
  END AS dia_da_semana,
 	bc.id as id_canal,
 	bc.snippet.title as nm_canal
from bronze_canais bc  ;

select 

	bv.statistics.viewcount as total_visualizacoes,
	bv.statistics.likecount as total_likes,
	bv.statistics.favoritecount  total_favoritos,
	bv.statistics.commentcount as total_comentarios,
	YEAR(bv.data_pesquisa) as ano,
	MONTH(bv.data_pesquisa) as mes,
	CASE dayofweek(bv.data_pesquisa)
	    WHEN 1 THEN 'Domingo'
	    WHEN 2 THEN 'Segunda-feira'
	    WHEN 3 THEN 'Terça-feira'
	    WHEN 4 THEN 'Quarta-feira'
	    WHEN 5 THEN 'Quinta-feira'
	    WHEN 6 THEN 'Sexta-feira'
	    WHEN 7 THEN 'Sábado'
  END AS dia_da_semana,
  bv.snippet.channelId as id_canal,
  bv.snippet.channelTitle as nome_canal,
  bv.id as id_video,
  bv.snippet.title as titulo_video
	
from bronze_videos bv ;




