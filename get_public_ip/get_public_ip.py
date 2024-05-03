import sys
import requests
import socket
from pyspark.sql import Row
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.taskcontext import TaskContext

# Get JOB_NAME from parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

def fetch_public_ip():
    try:
        response = requests.get("http://httpbin.org/ip")
        ip_info = response.json()
        return ip_info.get('origin')
    except requests.exceptions.RequestException as e:
        return "Failed to fetch IP: {}".format(e)

print("Driver IP: ", fetch_public_ip())

# Fetch IP of executor, along with hostname (internal IP) for comparison
def fetch_ip_task(_):
    context = TaskContext()
    ip = fetch_public_ip()
    hostname = socket.gethostname()
    return Row(ip_address=ip, hostname=hostname, partition_id=context.partitionId(), stage_id=context.stageId())

num_tasks = 10
tasks = spark.sparkContext.parallelize(range(num_tasks), num_tasks)

ip_results = tasks.map(fetch_ip_task).collect()
ip_df = spark.createDataFrame(ip_results)
ip_df.show()

job.commit()
