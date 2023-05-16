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

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://renee-stedi-project-lakehouse/accelerometer_trusted/"],
        "recurse": True,
    },
    transformation_ctx="accelerometer_trusted_node1",
)

# Script generated for node step_trainer_landing
step_trainer_landing_node1684277885644 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://renee-stedi-project-lakehouse/step_trainer_landing/"],
        "recurse": True,
    },
    transformation_ctx="step_trainer_landing_node1684277885644",
)

# Script generated for node customer_trusted_data
customer_trusted_data_node1684277965513 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://renee-stedi-project-lakehouse/customer_trusted_zone/"],
        "recurse": True,
    },
    transformation_ctx="customer_trusted_data_node1684277965513",
)

# Script generated for node JoinOnTimestamp
JoinOnTimestamp_node1684278045681 = Join.apply(
    frame1=accelerometer_trusted_node1,
    frame2=step_trainer_landing_node1684277885644,
    keys1=["timeStamp"],
    keys2=["sensorReadingTime"],
    transformation_ctx="JoinOnTimestamp_node1684278045681",
)

# Script generated for node JoinOnCustomer
JoinOnCustomer_node1684278126915 = Join.apply(
    frame1=customer_trusted_data_node1684277965513,
    frame2=JoinOnTimestamp_node1684278045681,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="JoinOnCustomer_node1684278126915",
)

# Script generated for node machine_learning_curated
machine_learning_curated_node3 = glueContext.write_dynamic_frame.from_options(
    frame=JoinOnCustomer_node1684278126915,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://renee-stedi-project-lakehouse/machine_learning_curated/",
        "partitionKeys": [],
    },
    transformation_ctx="machine_learning_curated_node3",
)

job.commit()
