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
AWSGlueDataCatalog_node1677717270183 = glueContext.create_dynamic_frame.from_catalog(
    database="datos_atmosfericos",
    table_name="temperatura",
    transformation_ctx="AWSGlueDataCatalog_node1677717270183",
)

# Script generated for node Change Schema (Apply Mapping)
ChangeSchemaApplyMapping_node1677717290704 = ApplyMapping.apply(
    frame=AWSGlueDataCatalog_node1677717270183,
    mappings=[
        ("codigoestacion", "long", "codigoestacion", "float"),
        ("codigosensor", "long", "codigosensor", "long"),
        ("fechaobservacion", "string", "fechaobservacion", "timestamp"),
        ("valorobservado", "double", "valorobservado", "double"),
        ("nombreestacion", "string", "nombreestacion", "string"),
        ("departamento", "string", "departamento", "string"),
        ("municipio", "string", "municipio", "string"),
        ("zonahidrografica", "string", "zonahidrografica", "string"),
        ("latitud", "double", "latitud", "double"),
        ("longitud", "double", "longitud", "double"),
        ("descripcionsensor", "string", "descripcionsensor", "string"),
        ("unidadmedida", "string", "unidadmedida", "string"),
    ],
    transformation_ctx="ChangeSchemaApplyMapping_node1677717290704",
)

# Script generated for node Filter
Filter_node1677717305120 = Filter.apply(
    frame=ChangeSchemaApplyMapping_node1677717290704,
    f=lambda row: (),
    transformation_ctx="Filter_node1677717305120",
)

# Script generated for node Amazon S3
AmazonS3_node1677717310679 = glueContext.write_dynamic_frame.from_options(
    frame=Filter_node1677717305120,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://trabajo-1/trusted/temperatura/",
        "partitionKeys": [],
    },
    transformation_ctx="AmazonS3_node1677717310679",
)

job.commit()
