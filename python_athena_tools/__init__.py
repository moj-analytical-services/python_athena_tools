import pandas as pd
import pyarrow.parquet as pq
import s3fs
import time
import uuid

def athena_query_to_pd_df(sql_query, out_path, timeout_seconds = 1e10, region='eu-west-1'):
    # Context manager tries to ensure that query results are deleted after being read.
    # This will fail if the user blows their RAM because Python will crash before cleaning up after itself
    with AthenaQuery(sql_query, out_path, timeout_seconds, region) as q:
        q.wait_for_completion()
        df = q.pd_read_results()
    return df

class AthenaQuery():

    #TODO:  Get region from AWS_DEFAULT_REGION env va
    def __init__(self, sql_query, out_path, timeout_seconds, region='eu-west-1'):
        self.sql_query = sql_query
        self.out_path = out_path
        self.response = None
        self.athena_client = boto3.client('athena', 'eu-west-1')
        self.s3_client = boto3.client('s3')
        self.s3_fs = s3fs.S3FileSystem()

    def __enter__(self):

        self.query_response = self.athena_client.start_query_execution(
            QueryString=sql_query,
            ResultConfiguration={'OutputLocation': self.out_path}
        )

        self.start_time = time.time()

        return self.reponse

    def add_create_table_as_to_sql(self):
        # TODO:  Check that it doesn't already have create table as

        additional_sql = """
        create table deleteme.deleteme{}
        WITH (
        format = 'PARQUET',
        parquet_compression = 'SNAPPY')
        as
        """.format(uuid.uuid4())

        return "{}{}".format(additional_sql, self.sql)



    def query_state(self):
        athena_status = self.athena_client(QueryExecutionId = self.query_response['QueryExecutionId'])
        return athena_status['QueryExecution']['Status']['State']

    def failed_reason(self):
        athena_status = self.athena_client(QueryExecutionId = self.query_response['QueryExecutionId'])
        return athena_status['QueryExecution']['Status']['StateChangeReason']

    def wait_for_completion(self):
        while True:

            if self.query_state() == "SUCCEEDED":
                break
            elif self.query_state() in ['QUEUED','RUNNING']:
                time.sleep(1)
            elif self.query_state() == 'FAILED':
                raise Exception("Athena failed - response error:\n {}".format(self.failed_reason()))
            else:
                raise Exception("Athena failed - unknown reason (printing full response):\n {athena_status}".format(athena_status))

            if self.timedout():
                raise TimeoutError('Your query took longer than the timeout you set')

    def timedout(self):
        elapsed_seconds = time.time() - self.start_time
        return elapsed_seconds > self.timeout_seconds

    def get_output_folder(self):
        return f"{output_location}/tables/{response['QueryExecutionId']}"

    def pd_read_results():
        output_folder =
        return pq.ParquetDataset(output_folder, filesystem=s3).read_pandas().to_pandas()

    def __exit__(self, *args):
        # Delete results
        output_location = self.response['QueryExecution']['ResultConfiguration']['OutputLocation']
        print(output_location)
        # boto3.delete_object(ouptut_location)

        # TODO: Also should delete the table from glue