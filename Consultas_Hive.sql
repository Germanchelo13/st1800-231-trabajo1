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
