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

# Script generated for node accelerometer_landing
accelerometer_landing_node1684185323443 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://renee-stedi-project-lakehouse/accelerometer_landing/"],
        "recurse": True,
    },
    transformation_ctx="accelerometer_landing_node1684185323443",
)

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

# Script generated for node InnerJoin
InnerJoin_node1684184631314 = Join.apply(
    frame1=customer_trusted_node1,
    frame2=accelerometer_landing_node1684185323443,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="InnerJoin_node1684184631314",
)

# Script generated for node DropExtraFields
DropExtraFields_node1684185763041 = DropFields.apply(
    frame=InnerJoin_node1684184631314,
    paths=[
        "serialNumber",
        "shareWithPublicAsOfDate",
        "birthDay",
        "registrationDate",
        "shareWithResearchAsOfDate",
        "customerName",
        "email",
        "lastUpdateDate",
        "phone",
        "shareWithFriendsAsOfDate",
    ],
    transformation_ctx="DropExtraFields_node1684185763041",
)

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1684185706039 = glueContext.write_dynamic_frame.from_options(
    frame=DropExtraFields_node1684185763041,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://renee-stedi-project-lakehouse/accelerometer_trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="accelerometer_trusted_node1684185706039",
)

job.commit()
