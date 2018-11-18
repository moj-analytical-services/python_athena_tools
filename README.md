# python_athena_tools
A wrapper around Athena to load query results into Python memory as fast as possible

Example usage:

```
from python_athena_tools import athena_query_to_pd_df

sql = """
    select * from
    flights_demo.flights_raw
    limit 10
    """

athena_query_to_pd_df(sql, "aws-athena-query-results-593291632749-eu-west-1")
```
