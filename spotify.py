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

# Script generated for node albums
albums_node1704516955446 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://project-spotify-trishita/staging/albums.csv"],
        "recurse": True,
    },
    transformation_ctx="albums_node1704516955446",
)

# Script generated for node artists
artists_node1704516956321 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://project-spotify-trishita/staging/artists.csv"],
        "recurse": True,
    },
    transformation_ctx="artists_node1704516956321",
)

# Script generated for node tracks
tracks_node1704516957273 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://project-spotify-trishita/staging/track.csv"],
        "recurse": True,
    },
    transformation_ctx="tracks_node1704516957273",
)

# Script generated for node Join
Join_node1704517056730 = Join.apply(
    frame1=albums_node1704516955446,
    frame2=artists_node1704516956321,
    keys1=["artist_id"],
    keys2=["id"],
    transformation_ctx="Join_node1704517056730",
)

# Script generated for node Join
Join_node1704517260890 = Join.apply(
    frame1=Join_node1704517056730,
    frame2=tracks_node1704516957273,
    keys1=["track_id"],
    keys2=["track_id"],
    transformation_ctx="Join_node1704517260890",
)

# Script generated for node Drop Fields
DropFields_node1704517366051 = DropFields.apply(
    frame=Join_node1704517260890,
    paths=["`.track_id`", "id"],
    transformation_ctx="DropFields_node1704517366051",
)

# Script generated for node Amazon S3
AmazonS3_node1704517396119 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1704517366051,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://project-spotify-trishita/datawarehouse/",
        "partitionKeys": [],
    },
    format_options={"compression": "snappy"},
    transformation_ctx="AmazonS3_node1704517396119",
)

job.commit()
