-- Canais
--- Total visualizacoes_dia selecionando canal

drop VIEW ouro_canal_total_visualizacao_dia;
create VIEW ouro_canal_total_visualizacao_dia as
select 
	pc.total_visualizacoes - LAG(pc.total_visualizacoes, 1, 0) OVER (PARTITION BY pc.id_canal ORDER BY pc.mes, pc.dia) as TOTAL_VISUALIZACAO_DIA,
	pc.nm_canal as nome_canal,
	pc.semana as semana,
	pc.assunto as assunto,
	pc.ano as ano,
	pc.mes as mes,
	pc.dia as dia,
	case when pc.semana == 'Domingo'
		then 0
	when pc.semana == 'Segunda-feira'
		then 1
	when pc.semana == 'Terça-feira'	
		then 2
	when pc.semana == 'Quarta-feira'	
		then 3
	when pc.semana == 'Quinta-feira'	
		then 4
	when pc.semana == 'Sexta-feira'	
		then 5
	when pc.semana == 'Sábado'	
		then 6 end as indice_semana,
    pc.id_canal as id_canal
from prata_canal pc 
 -- where pc.id_canal = 'UCGxmd2AnLNrSWFhrCcEj0lQ'  
-- AND  pc.dia BETWEEN 24 and 25  and pc.mes in (5, 6)
 -- where DATE_FORMAT(CONCAT_WS('-', CAST(pc.ano AS STRING), LPAD(CAST(pc.mes AS STRING), 2, '0'), LPAD(CAST(pc.dia AS STRING), 2, '0')), 'yyyy-MM-dd') >= CURRENT_DATE() - 1
order by pc.id_canal , pc.mes, pc.dia ;

select *
from ouro_canal_total_visualizacao_dia
where id_canal = 'UCGxmd2AnLNrSWFhrCcEj0lQ'  ;

  
----- Taxa Engajamento do canal por dia: (total_visualizacoes / total_videos_publicados) / total_inscritos

select 
	pc.id_canal, 
	pc.mes, 
	pc.dia,
	pc.total_visualizacoes - LAG(pc.total_visualizacoes, 1, 0) OVER (PARTITION BY pc.id_canal ORDER BY pc.mes, pc.dia) as TOTAL_VISUALIZACAO_DIA,
	pc.total_videos_publicados  - LAG(pc.total_videos_publicados, 1, 0) OVER (PARTITION BY pc.id_canal ORDER BY pc.mes, pc.dia) as TOTAL_VIDEOS_PUBLICADOS_DIA,
	pc.total_inscritos - LAG(pc.total_inscritos, 1, 0) OVER (PARTITION BY pc.id_canal ORDER BY pc.mes, pc.dia) as TOTAL_INSCRITOS_DIA,
	(pc.total_inscritos - LAG(pc.total_inscritos, 1, 0) OVER (PARTITION BY pc.id_canal ORDER BY pc.mes, pc.dia) /  pc.total_visualizacoes - LAG(pc.total_visualizacoes, 1, 0) OVER (PARTITION BY pc.id_canal ORDER BY pc.mes, pc.dia)) * 100
from prata_canal pc 
order by pc.id_canal, pc.mes, pc.dia;
 
select *
from prata_canal;
 

--- Total Vídeos Públicados dia selecionando canal



create VIEW ouro_canal_total_videos_publicados_dia as
select 
	pc.total_videos_publicados - LAG(pc.total_videos_publicados, 1, 0) OVER (PARTITION BY pc.id_canal ORDER BY pc.mes, pc.dia) as TOTAL_VIDEOS_PUBLICADOS_DIA,
	pc.nm_canal as nome_canal,
	pc.semana as semana,
	pc.assunto as assunto,
	pc.ano as ano,
	pc.mes as mes,
	pc.dia as dia,
	case when pc.semana == 'Domingo'
		then 0
	when pc.semana == 'Segunda-feira'
		then 1
	when pc.semana == 'Terça-feira'	
		then 2
	when pc.semana == 'Quarta-feira'	
		then 3
	when pc.semana == 'Quinta-feira'	
		then 4
	when pc.semana == 'Sexta-feira'	
		then 5
	when pc.semana == 'Sábado'	
		then 6 end as indice_semana,
    pc.id_canal as id_canal
from prata_canal pc 
order by pc.id_canal , pc.mes, pc.dia ;

select *
from ouro_canal_total_videos_publicados_dia;


--- total Inscritos dia selecionando canal


select *
from prata_canal;

drop VIEW ouro_canal_total_inscritos_dia

create VIEW ouro_canal_total_inscritos_dia as
select 
	pc.total_inscritos - LAG(pc.total_inscritos, 1, 0) OVER (PARTITION BY pc.id_canal ORDER BY pc.mes, pc.dia) as TOTAL_INSCRITOS_DIA,
	pc.nm_canal as nome_canal,
	pc.semana as semana,
	pc.assunto as assunto,
	pc.ano as ano,
	pc.mes as mes,
	pc.dia as dia,
	case when pc.semana == 'Domingo'
		then 0
	when pc.semana == 'Segunda-feira'
		then 1
	when pc.semana == 'Terça-feira'	
		then 2
	when pc.semana == 'Quarta-feira'	
		then 3
	when pc.semana == 'Quinta-feira'	
		then 4
	when pc.semana == 'Sexta-feira'	
		then 5
	when pc.semana == 'Sábado'	
		then 6 end as indice_semana,
    pc.id_canal as id_canal
from prata_canal pc 
order by pc.id_canal , pc.mes, pc.dia ;


select *
from ouro_canal_total_inscritos_dia;
--- Taxa engajamento canal por dia

create view ouro_canal_taxa_engajamento_dia as 
select 
	pc.id_canal , 
	pc.nm_canal ,
	pc.mes, 
	pc.dia,
	pc.total_visualizacoes,
	pc.total_inscritos,
	pc.semana, 
	case when pc.semana == 'Domingo'
		then 0
	when pc.semana == 'Segunda-feira'
		then 1
	when pc.semana == 'Terça-feira'	
		then 2
	when pc.semana == 'Quarta-feira'	
		then 3
	when pc.semana == 'Quinta-feira'	
		then 4
	when pc.semana == 'Sexta-feira'	
		then 5
	when pc.semana == 'Sábado'	
		then 6 end as indice_semana,

    pc.total_visualizacoes - LAG(pc.total_visualizacoes, 1, 0) OVER (PARTITION BY pc.id_canal ORDER BY pc.mes, pc.dia), 
	round((pc.total_visualizacoes / pc.total_inscritos ) * 100, 2) as taxa_engajamento_canal
	
from prata_canal pc 
order by pc.id_canal , pc.mes, pc.dia ;

select *
from ouro_canal_taxa_engajamento_dia


--- Videos
--- Total Visualizacoes dia
drop VIEW ouro_video_total_visualizacoes_dia;

create VIEw ouro_video_total_visualizacoes_dia as 
select 
	pv.assunto as assunto,
	pv.id_canal as id_canal ,
	pv.nome_canal as nome_canal,
	pv.id_video as id_video,
	pv.titulo_video,
	pv.mes as mes,
	pv.dia as dia,
	pv.semana,
	case when pv.semana == 'Domingo'
		then 0
	when pv.semana == 'Segunda-feira'
		then 1
	when pv.semana == 'Terça-feira'	
		then 2
	when pv.semana == 'Quarta-feira'	
		then 3
	when pv.semana == 'Quinta-feira'	
		then 4
	when pv.semana == 'Sexta-feira'	
		then 5
	when pv.semana == 'Sábado'	
		then 6 end as indice_semana,
	pv.total_visualizacoes - LAG(pv.total_visualizacoes, 1, 0) OVER (PARTITION BY pv.id_video ORDER BY pv.mes, pv.dia) as total_visualizacoes_dia
from prata_video pv 
order by pv.id_video, pv.mes, pv.dia;


select * from prata_video pv ;

select *
from ouro_video_total_visualizacoes_dia pv 
where pv.id_video  = 'uhtx7w9MyVo';

--- TOTAL COMENTÁRIOS DIA
drop VIEW ouro_video_total_vcomentarios_dia;

create VIEw ouro_video_total_vcomentarios_dia as 
select 
	pv.assunto as assunto,
	pv.id_canal as id_canal ,
	pv.nome_canal as nome_canal,
	pv.id_video as id_video,
	pv.titulo_video,
	pv.mes as mes,
	pv.dia as dia,
	pv.semana,
	case when pv.semana == 'Domingo'
		then 0
	when pv.semana == 'Segunda-feira'
		then 1
	when pv.semana == 'Terça-feira'	
		then 2
	when pv.semana == 'Quarta-feira'	
		then 3
	when pv.semana == 'Quinta-feira'	
		then 4
	when pv.semana == 'Sexta-feira'	
		then 5
	when pv.semana == 'Sábado'	
		then 6 end as indice_semana,
	pv.total_comentarios - LAG(pv.total_comentarios, 1, 0) OVER (PARTITION BY pv.id_video ORDER BY pv.mes, pv.dia) as total_comentarios_dia
from prata_video pv 
order by pv.id_video, pv.mes, pv.dia;


-- TOTAL LIKES 
drop VIEW ouro_video_total_likes_dia;

create VIEW ouro_video_total_likes_dia as 
select 
	pv.assunto as assunto,
	pv.id_canal as id_canal ,
	pv.nome_canal as nome_canal,
	pv.id_video as id_video,
	pv.titulo_video,
	pv.mes as mes,
	pv.dia as dia,
	pv.semana,
	case when pv.semana == 'Domingo'
		then 0
	when pv.semana == 'Segunda-feira'
		then 1
	when pv.semana == 'Terça-feira'	
		then 2
	when pv.semana == 'Quarta-feira'	
		then 3
	when pv.semana == 'Quinta-feira'	
		then 4
	when pv.semana == 'Sexta-feira'	
		then 5
	when pv.semana == 'Sábado'	
		then 6 end as indice_semana,
	pv.total_likes - LAG(pv.total_likes, 1, 0) OVER (PARTITION BY pv.id_video ORDER BY pv.mes, pv.dia) as total_likes_dia
from prata_video pv 
order by pv.id_video, pv.mes, pv.dia;


select * from ouro_video_total_likes_dia
-- taxa engajamento do vídeo por dia
drop view ouro_video_taxa_engajamento_video_dia 

create VIEW ouro_video_taxa_engajamento_video_dia AS
select 
	pv.assunto ,
	pv.nome_canal,
	pv.titulo_video,
	pv.id_video, 
	pv.mes, 
	pv.dia,
	pv.semana,
	case when pv.semana == 'Domingo'
		then 0
	when pv.semana == 'Segunda-feira'
		then 1
	when pv.semana == 'Terça-feira'	
		then 2
	when pv.semana == 'Quarta-feira'	
		then 3
	when pv.semana == 'Quinta-feira'	
		then 4
	when pv.semana == 'Sexta-feira'	
		then 5
	when pv.semana == 'Sábado'	
		then 6 end as indice_semana,

	coalesce(round((
		(
			(pv.total_likes - LAG(pv.total_likes, 1, 0) OVER (PARTITION BY pv.id_video ORDER BY pv.mes, pv.dia)) + 
			(pv.total_comentarios - LAG(pv.total_comentarios, 1, 0) OVER (PARTITION BY pv.id_video ORDER BY pv.mes, pv.dia)) 
		
		) / 
			(pv.total_visualizacoes - LAG(pv.total_visualizacoes, 1, 0) OVER (PARTITION BY pv.id_video ORDER BY pv.mes, pv.dia))
	) * 100, 2), 0)
from prata_video pv
order by pv.id_video, pv.mes, pv.dia;

select *
from ouro_video_taxa_engajamento_video_dia pv
where pv.assunto = 'palworld';

select *
from ouro_video_taxa_engajamento_video_dia

create VIEW depara_video as 
select distinct
	pv.assunto ,
	pv.id_canal,
	pv.nome_canal ,
	pv.id_video,
	pv.titulo_video
from prata_video pv 
order by id_canal ;

create view depara_canal as 
select 
	DISTINCT
	pc.assunto,
	pc.id_canal,
	pc.nm_canal
from prata_canal pc 
order by pc.id_canal;

select *
from depara_canal;

select *
from depara_video dv 

