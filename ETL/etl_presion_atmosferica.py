import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1677717433390 = glueContext.create_dynamic_frame.from_catalog(
    database="datos_atmosfericos",
    table_name="presion_atmosferica",
    transformation_ctx="AWSGlueDataCatalog_node1677717433390",
)

# Script generated for node Change Schema (Apply Mapping)
ChangeSchemaApplyMapping_node1677719838129 = ApplyMapping.apply(
    frame=AWSGlueDataCatalog_node1677717433390,
    mappings=[
        ("codigoestacion", "long", "codigoestacion", "long"),
        ("codigosensor", "long", "codigosensor", "long"),
        ("fechaobservacion", "string", "fechaobservacion", "timestamp"),
        ("valorobservado", "double", "valorobservado", "float"),
        ("nombreestacion", "string", "nombreestacion", "string"),
        ("departamento", "string", "departamento", "string"),
        ("municipio", "string", "municipio", "string"),
        ("zonahidrografica", "string", "zonahidrografica", "string"),
        ("latitud", "double", "latitud", "double"),
        ("longitud", "double", "longitud", "double"),
        ("descripcionsensor", "string", "descripcionsensor", "string"),
        ("unidadmedida", "string", "unidadmedida", "string"),
    ],
    transformation_ctx="ChangeSchemaApplyMapping_node1677719838129",
)

# Script generated for node Filter
Filter_node1677717452112 = Filter.apply(
    frame=ChangeSchemaApplyMapping_node1677719838129,
    f=lambda row: (row["valorobservado"] >= 885 and row["valorobservado"] <= 1077),
    transformation_ctx="Filter_node1677717452112",
)

# Script generated for node Amazon S3
AmazonS3_node1677717457655 = glueContext.write_dynamic_frame.from_options(
    frame=Filter_node1677717452112,
    connection_type="s3",
    format="csv",
    connection_options={
        "path": "s3://trabajo-1/trusted/presion_atmosferica/",
        "partitionKeys": [],
    },
    transformation_ctx="AmazonS3_node1677717457655",
)

job.commit()
