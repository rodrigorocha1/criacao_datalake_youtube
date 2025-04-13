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

SELECT * FROM hive_partition_by hpb ;



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
LOCATION '/employee';

CREATE  TABLE employee_external_json (
    name STRING
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'/


SELECT
 FROM_UNIXTIME(UNIX_TIMESTAMP()) AS current_time
select name:name
FROM employee_external_json;

SELECT get_json_object(name, '$.name') AS extracted_name
FROM employee_external_json;

INSERT INTO employee_external_json
SELECT '{"name": "John Doe"}' AS name;


LOAD DATA LOCAL INPATH '/employee/employee.txt'
INTO TABLE employee_external

select *
FROM employee_external;

LOAD DATA LOCAL INPATH '/teste_external_table/employee.txt'
OVERWRITE INTO TABLE employee_external;

LOAD DATA LOCAL INPATH '/teste_external_table/employee.txt'
APPEND INTO TABLE employee_external;

DROP TABLE TESTE;


DROP TABLE pessoas;

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


CREATE TABLE your_table (
    id INT,
    name STRING
)
PARTITIONED BY (city STRING)  -- Particionando pela coluna 'city'
STORED AS PARQUET;  -- Utilizando o formato de armazenamento Parquet


