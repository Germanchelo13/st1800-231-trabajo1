create external schema myspectrum_schema
from data catalog
database 'datos_atmosfericos'
iam_role 'arn:aws:iam::409340898205:role/LabRole'
create external database if not exists;

DROP TABLE IF EXISTS myspectrum_schema.presion_atmosferica;
create external table myspectrum_schema.presion_atmosferica(
mes varchar,
ano varchar,
valorobservado decimal,
departamento varchar,
municipio varchar,
latitud decimal,
longitud decimal
)
row format delimited
fields terminated by '\t'
stored as textfile
location 's3://trabajo-1/trusted/presion_atmosferica/'
table properties ('numRows'='172000');
select count(*)from myspectrum_schema.presion_atmosferica;
