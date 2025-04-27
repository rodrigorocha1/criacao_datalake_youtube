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


select *
from bronze_videos bv ;




