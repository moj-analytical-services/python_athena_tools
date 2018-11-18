# python_athena_tools

A wrapper around Athena to load query results into Python memory as fast as possible

This is particularly suited to queries which return large amounts of data.

Example usage:

```
from python_athena_tools import athena_query_to_pd_df

sql = """
    select * from
    mydatabase.mytable
    limit 10
    """

athena_query_to_pd_df(sql, "bucket-i-have-readwrite-permissions", region="eu-west-1")
```

Our strategy is to use Athena's [Create Table As](https://docs.aws.amazon.com/athena/latest/ug/create-table-as.html) to run the query and dump the results out to Parquet format in a temporary folder.  We then use Apache Arrow to read the parquet files into pandas directly from s3.

You'll therefore need the following IAM permissions:

- Create items in glue catalogue (required for create table as)
- Delete items from the glue catalogue (we tidy up after ourselves so the table will be deleted once read)
- Create items in the s3 bucket, specifically a folder within it called `__athena_temp__`.
- Delete items from the s3 bucket, specifically a folder within it called `__athena_temp__`.

We use a context manager to perform the cleanup.  This works if the query runs successfully or fails due to a timeout/syntax error etc.  However, if blow your RAM when running a big query and crash Python, it probably won't perform this cleanup.