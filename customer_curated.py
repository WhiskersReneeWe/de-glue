import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node customer_trusted
customer_trusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://renee-stedi-project-lakehouse/customer_trusted_zone/"],
        "recurse": True,
    },
    transformation_ctx="customer_trusted_node1",
)

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1684196065076 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://renee-stedi-project-lakehouse/accelerometer_trusted/"],
        "recurse": True,
    },
    transformation_ctx="accelerometer_trusted_node1684196065076",
)

# Script generated for node joinOnCustomer
joinOnCustomer_node1684196122022 = Join.apply(
    frame1=customer_trusted_node1,
    frame2=accelerometer_trusted_node1684196065076,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="joinOnCustomer_node1684196122022",
)

# Script generated for node dropAccelerometerFields
dropAccelerometerFields_node1684196278517 = DropFields.apply(
    frame=joinOnCustomer_node1684196122022,
    paths=["timeStamp", "y", "x", "user", "z"],
    transformation_ctx="dropAccelerometerFields_node1684196278517",
)

# Script generated for node customer_curated
customer_curated_node3 = glueContext.write_dynamic_frame.from_options(
    frame=dropAccelerometerFields_node1684196278517,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://renee-stedi-project-lakehouse/customers_curated/",
        "partitionKeys": [],
    },
    transformation_ctx="customer_curated_node3",
)

job.commit()
