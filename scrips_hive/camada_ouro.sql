select pv.id_canal, pv.total_videos_publicados
from prata_canal pv   
where pv.assunto = 'palworld'
order by pv.id_canal ;

DESCRIBE prata_canal;
describe FORMATED prata_canal;
describe EXTENDED prata_canal;



EXPLAIN
select pv.id_canal, pv.total_videos_publicados
from prata_canal pv
where pv.assunto = 'palworld'
ORDER BY pv.id_canal 

SELECT * FROM prata_canal LIMIT 10;



SELECT 
    pv.id_canal, 
    pv.total_videos_publicados
FROM prata_canal pv
WHERE pv.assunto = 'palworld'
LIMIT 10;