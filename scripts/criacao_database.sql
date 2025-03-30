CREATE DATABASE youtube_datalake;
use youtube_datalake;

CREATE EXTERNAL TABLE employee_external (
    name STRING,
    workplace ARRAY<STRING>,
    sex_age STRUCT<sex:STRING, age:INT>,
    skills_score MAP<STRING, INT>,
    depart_tittle MAP<STRING, ARRAY<STRING>>
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
COLLECTION ITEMS TERMINATED BY ','
MAP KEYS TERMINATED BY ':'
STORED AS TEXTFILE
LOCATION '/hadoop/temp/';

select *
FROM employee_external;

DROP TABLE employee_external ;

SELECT '1';



CREATE EXTERNAL TABLE employee_external (
    name STRING,
    work_place ARRAY<STRING>,
    sex_age STRUCT<sex:STRING, age:INT>,
    skills_score MAP<STRING, INT>,
    depart_title MAP<STRING, ARRAY<STRING>>
)
COMMENT 'This is an external table'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
COLLECTION ITEMS TERMINATED BY ','
MAP KEYS TERMINATED BY ':'
STORED AS TEXTFILE
LOCATION '/teste';


LOAD DATA LOCAL INPATH '/hadoop/temp/employee.txt'
OVERWRITE
INTO TABLE employee_external



LOAD DATA LOCAL INPATH '/teste_external_table/employee.txt'
OVERWRITE INTO TABLE employee_external;

DROP TABLE TESTE;


CREATE EXTERNAL TABLE pessoas (
  id INT,
  name STRING,
  age INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/teste';  


LOAD DATA LOCAL INPATH '/pessoas.csv' INTO TABLE pessoas;

LOAD DATA INPATH '/root/teste/pessoas.csv' INTO TABLE pessoas;

