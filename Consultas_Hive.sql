# tabla externa en S3: 
use user1;
CREATE EXTERNAL TABLE temperatura (codigoestacion STRING, 
CodigoSensor STRING, 
FechaObservacion STRING,
ValorObservado FLOAT, 
NombreEstacion STRING
, Departamento STRING, 
Municipio STRING,ZonaHidrografica STRING, Latitud FLOAT,Longitud FLOAT, DescripcionSensor STRING, UnidadMedida STRING)  
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE 
LOCATION 's3://trabajo-1/raw/temperatura/'
-- Consulta para datos de temperatura
SELECT replace(temp.departamento, '"', '') as departamento,
	temp.ano as ano,
	avg(temp.valorobservado) as temperatura
FROM trusted_temperatura as temp
WHERE ano = 2021
group by temp.ano, temp.departamento
order by temperatura desc
limit 10;
-- Fin de consulta
-- Consulta para datos de presion 
SELECT replace(temp.municipio, '"', '') as municipio,
	temp.ano as ano,
	avg(temp.valorobservado) as presion_atmosferica
FROM trusted_presion_atmosferica as temp
WHERE temp.ano = 2021
group by temp.ano, municipio
order by presion_atmosferica desc
limit 10;
-- fin de consulta