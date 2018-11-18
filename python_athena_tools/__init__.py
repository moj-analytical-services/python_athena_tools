import boto3
import pandas as pd
import pyarrow.parquet as pq
import s3fs
import time
import uuid

from botocore.client import ClientError

def athena_query_to_pd_df(sql_query, temp_bucket, timeout_seconds = 1e10, region='eu-west-1'):
    """Query Athena and return results as a pandas dataframe

    Args:
        sql_query: The sql 'select' query to run
        temp_bucket: The temp_bucket to use as a temporary storage area.  An '__athena_temp__' directory will be
                     temporarily created and then deleted.
        timeout_seconds:  If set, do not let the query run for longer than this.
        region: The region in which the glue database resides
    Returns:
        A pandas dataframe containing the query results
    """
    # The context manager tries to ensure that query results are deleted after being read into memory.
    # Note this isn't 100% reliable:  it fail if the user blows their RAM because Python will crash before cleaning up after itself
    with AthenaQuery(sql_query, temp_bucket, timeout_seconds, region) as q:
        q.wait_for_completion()
        return q.pd_read_results()

class AthenaQuery():

    #TODO:  Get region from AWS_DEFAULT_REGION env variable
    def __init__(self, sql_query, temp_bucket, timeout_seconds = 1e10, region='eu-west-1'):
        self.verify_sql(sql_query)

        self.s3_client = boto3.client('s3')
        self.temp_bucket = temp_bucket
        self.verify_bucket(temp_bucket)

        self.base_output_path = "s3://{}/__athena_temp__".format(temp_bucket)
        self.raw_sql_query = sql_query
        self.timeout_seconds = timeout_seconds
        self.response = None
        self.table_name = None
        self.athena_client = boto3.client('athena', region)
        self.glue_client = boto3.client('glue', region)
        self.s3_resource = boto3.resource('s3')

        self.boto3_bucket = self.s3_resource.Bucket(self.temp_bucket)
        self.s3_fs = s3fs.S3FileSystem()

    def __enter__(self):
        self.query_response = self.athena_client.start_query_execution(
            QueryString=self.sql_with_create_table(),
            ResultConfiguration={'OutputLocation': self.base_output_path}
        )

        self.start_time = time.time()
        return self

    def verify_sql(self, sql_query):
        sql_temp = sql_query.strip().lower()
        if not sql_temp.startswith("select"):
            raise ValueError("The sql statement must be a select query i.e. it must start with the token 'select'")

    def verify_bucket(self, bucket):
        if "/" in bucket or ":" in bucket:
            raise ValueError("Bucket should be the bucket name only e.g. alpha-mydata.  It should not include s3:// or anything else")

        try:
            self.s3_client.head_bucket(Bucket=self.temp_bucket)
        except ClientError:
            # The bucket does not exist or you have no access.
            raise Exception("The bucket you specified does not exist or you don't have access to it")

    def sql_with_create_table(self):



        uid = str(uuid.uuid4()).replace("-", "")
        self.table_name = "deleteme{}".format(uid)
        additional_sql = """
            create table deleteme.{}
            WITH (
            format = 'PARQUET',
            parquet_compression = 'SNAPPY')
            as
        """.format(self.table_name)

        return "{}{}".format(additional_sql, self.raw_sql_query)

    def query_state(self):
        athena_status = self.athena_client.get_query_execution(QueryExecutionId = self.query_response['QueryExecutionId'])
        return athena_status['QueryExecution']['Status']['State']

    def failed_reason(self):
        athena_status = self.athena_client.get_query_execution(QueryExecutionId = self.query_response['QueryExecutionId'])
        return athena_status['QueryExecution']['Status']['StateChangeReason']

    def wait_for_completion(self):
        while True:
            query_state = self.query_state()
            if query_state == "SUCCEEDED":
                break
            elif query_state in ['QUEUED','RUNNING']:
                time.sleep(1)
            elif query_state == 'FAILED':
                raise Exception("Athena failed - response error:\n {}".format(self.failed_reason()))
            else:
                athena_status = self.athena_client.get_query_execution(QueryExecutionId = self.query_response['QueryExecutionId'])
                raise Exception("Athena failed - unknown reason (printing full response):\n {athena_status}".format(athena_status))

            if self.timedout():
                self.athena_client.stop_query_execution(QueryExecutionId=self.query_response['QueryExecutionId'])
                raise TimeoutError('Your query took longer than the timeout you set')

    def timedout(self):
        elapsed_seconds = time.time() - self.start_time
        return elapsed_seconds > self.timeout_seconds

    def output_folder_full_path(self):
        execution_id = self.query_response['QueryExecutionId']
        return "{}/tables/{}".format(self.base_output_path, execution_id)

    def output_folder_not_including_bucket(self):
        full_path = self.output_folder_full_path()
        return full_path.replace("s3://{}/".format(self.temp_bucket), "")

    def pd_read_results(self):
        return pq.ParquetDataset(self.output_folder_full_path(), filesystem=self.s3_fs).read_pandas().to_pandas()

    def __exit__(self, *args):
        # Clean up query results and table in glue catalogue
        prefix = self.output_folder_not_including_bucket()
        if "__athena_temp__" not in prefix:
            raise Exception("User has edited prefix, exiting before deleting temprary files and table for safety")
        self.boto3_bucket.objects.filter(Prefix=prefix).delete()
        self.glue_client.delete_table(DatabaseName="deleteme", Name=self.table_name)
