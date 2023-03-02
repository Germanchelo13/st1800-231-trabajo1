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
    table_name="tiendas_sostenibles",
    transformation_ctx="AWSGlueDataCatalog_node1677717433390",
)

# Script generated for node Change Schema (Apply Mapping)
ChangeSchemaApplyMapping_node1677717444807 = ApplyMapping.apply(
    frame=AWSGlueDataCatalog_node1677717433390,
    mappings=[
        ("año", "long", "año", "long"),
        ("autoridad ambiental", "string", "autoridad ambiental", "string"),
        ("región", "string", "región", "string"),
        ("departamento", "string", "departamento", "string"),
        ("municipio", "string", "municipio", "string"),
        ("razón social", "string", "razón social", "string"),
        ("descripción", "string", "descripción", "string"),
        ("categoría", "string", "categoría", "string"),
        ("sector", "string", "sector", "string"),
        ("subsector", "string", "subsector", "string"),
        ("producto principal", "string", "producto principal", "string"),
        ("nombre representante", "string", "nombre representante", "string"),
        ("resultado", "string", "resultado", "string"),
    ],
    transformation_ctx="ChangeSchemaApplyMapping_node1677717444807",
)

# Script generated for node Filter
Filter_node1677717452112 = Filter.apply(
    frame=ChangeSchemaApplyMapping_node1677717444807,
    f=lambda row: (bool(re.match("Antioquia", row["departamento"]))),
    transformation_ctx="Filter_node1677717452112",
)

# Script generated for node Amazon S3
AmazonS3_node1677717457655 = glueContext.write_dynamic_frame.from_options(
    frame=Filter_node1677717452112,
    connection_type="s3",
    format="csv",
    connection_options={"path": "s3://trabajo-1/trusted/tiendas/", "partitionKeys": []},
    transformation_ctx="AmazonS3_node1677717457655",
)

job.commit()
