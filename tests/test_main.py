# -*- coding: utf-8 -*-

import unittest
from python_athena_tools import athena_query_to_pd_df, AthenaQuery

class TestAthenaQueryToDf(unittest.TestCase):

    valid_sql = """
        select * from
        flights_demo.flights_raw
        limit 10
        """

    invalid_sql_cta = """
        create table mydb.mytable as
        select * from
        flights_demo.flights_raw
        limit 10
        """

    temp_bucket = "aws-athena-query-results-593291632749-eu-west-1"

    with AthenaQuery(valid_sql, temp_bucket) as q:
        q.wait_for_completion()
        q.pd_read_results()

    athenaquery = q

    def test_query_doesnt_start_with_select(self):
        with self.assertRaises(ValueError) as context:
            athena_query_to_pd_df(self.invalid_sql_cta, self.temp_bucket)

    def test_wrongbucket(self):
        with self.assertRaises(Exception) as context:
            athena_query_to_pd_df(self.valid_sql, "blah-blah-bad-bucket")

    def test_timeout(self):
        with self.assertRaises(TimeoutError) as context:
            athena_query_to_pd_df(self.valid_sql, self.temp_bucket, timeout_seconds=1)

    # No files should exist after running
    def test_removes_files(self):
        prefix = self.athenaquery.output_folder_not_including_bucket()
        objs = self.athenaquery.boto3_bucket.objects.filter(Prefix=prefix)

        self.assertTrue(len(list(objs)) == 0)

    def test_execute_valid_query(self):
        athena_query_to_pd_df(self.valid_sql, self.temp_bucket)

if __name__ == '__main__':
    unittest.main()