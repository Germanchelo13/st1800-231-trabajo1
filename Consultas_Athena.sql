-- Consulta para datos de temperatura
SELECT replace(temp.departamento, '"', '') as departamento,
	temp.ano as ano,
	avg(temp.valorobservado) as temperatura
FROM "datos_atmosfericos"."trusted_temperatura" as temp
WHERE ano = 2021
group by (temp.ano, temp.departamento)
order by temperatura desc
limit 10;
-- Fin de consulta
-- Consulta para datos de presion 
SELECT replace(temp.municipio, '"', '') as municipio,
	temp.ano as ano,
	avg(temp.valorobservado) as presion_atmosferica
FROM "datos_atmosfericos"."trusted_presion_atmosferica" as temp
WHERE temp.ano = 2021
group by (temp.ano, municipio)
order by presion_atmosferica desc
limit 10;
-- fin de consulta