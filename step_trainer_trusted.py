import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node step trainer landing
steptrainerlanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://renee-stedi-project-lakehouse/step_trainer_landing/"],
        "recurse": True,
    },
    transformation_ctx="steptrainerlanding_node1",
)

# Script generated for node customers curated
customerscurated_node1684202671717 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://renee-stedi-project-lakehouse/customers_curated"],
        "recurse": True,
    },
    transformation_ctx="customerscurated_node1684202671717",
)

# Script generated for node rightJoin
steptrainerlanding_node1DF = steptrainerlanding_node1.toDF()
customerscurated_node1684202671717DF = customerscurated_node1684202671717.toDF()
rightJoin_node1684202726969 = DynamicFrame.fromDF(
    steptrainerlanding_node1DF.join(
        customerscurated_node1684202671717DF,
        (
            steptrainerlanding_node1DF["serialNumber"]
            == customerscurated_node1684202671717DF["serialNumber"]
        ),
        "right",
    ),
    glueContext,
    "rightJoin_node1684202726969",
)

# Script generated for node step_trainer_trusted
step_trainer_trusted_node3 = glueContext.write_dynamic_frame.from_options(
    frame=rightJoin_node1684202726969,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://renee-stedi-project-lakehouse/step_trainer_trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="step_trainer_trusted_node3",
)

job.commit()
