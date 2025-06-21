-- Canais
--- Total visualizacoes_dia dia

select *
from prata_canal pc 
order by pc.id_canal;

select *
from bronze_canais bc 
order by bc.id, bc.mes, bc.dia ;

select *
from bronze_assunto bv  


 ALTER TABLE bronze_assunto
            ADD IF NOT EXISTS PARTITION (
                ano=2025,
                mes=5,
                dia=25,
                dia_semana='Domingo',
                assunto="python"
            )
            
            
show partitions bronze_videos

select 
	  ano
	, mes
	, dia
	, assunto
	, id_canal
	, total_videos_publicados
from prata_canal pc 
order by id_canal ;

--- Total Commentarios dia



--- total vísualizacoes dia


--- Total engajamento 


select pc.ano, 
pc.mes, 
pc.dia, 
pc.assunto, 
pc.nm_canal , 
pc.id_canal ,
(pc.total_visualizacoes / pc.total_inscritos) * 100 as total_engajmento
from prata_canal pc 
order by  pc.id_canal 


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

ALTER TABLE bronze_videos DROP IF EXISTS PARTITION (
    ano=2025,
    mes=5,
    dia=25,
    dia_semana='Domingo',
    assunto='no_mans_sky'
);

ALTER TABLE bronze_videos DROP IF EXISTS PARTITION (
    ano=2025,
    mes=5,
    dia=25,
    dia_semana='Domingo',
    assunto='palworld'
);

ALTER TABLE bronze_videos DROP IF EXISTS PARTITION (
    ano=2025,
    mes=5,
    dia=25,
    dia_semana='Domingo',
    assunto='python'
);





