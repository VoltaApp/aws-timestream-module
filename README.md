# Amazon Timestream Module

## Install this package
```pip install git+ssh://git@github.com/VoltaApp/aws-timestream-module.git@<branch_name>```

## Example usages

### Write
```python
from datetime import datetime
from aws_timestream_module.services.write import WriteService

write_service = WriteService(
    # Change the default config here
    # write_client=boto3.Session().client(
    #     "timestream-write",
    #     region_name="ap-southeast-2",
    #     config=Config(
    #         read_timeout=20,
    #         max_pool_connections=5000,
    #         retries={"max_attempts": 10}
    #     )
    # )
)

# Init timestream items from dict items
timestream_items = write_service.get_records_with_multi_type(
    measure_name="example",
    time_field="time_field",
    dimension_fields=["example_field_1"],
    measure_value_fields=["example_field_2"],
    items=[
        {
            "example_field_1": "example_value",
            "time_field": datetime.utcnow(),
            "example_field_2": "example_value"
        }
    ]
)
# output:
# [{'MeasureName': 'example', 'Dimensions': [{'Name': 'example_field_1', 'Value': 'example_value'}], 'MeasureValueType': 'MULTI', 'Time': '1683237700176', 'MeasureValues': [{'Name': 'example_field_2', 'Value': 'example_value', 'Type': 'VARCHAR'}]}]

​# Ingest timestream items
write_service.write_records(
    database_name="your_database_name",
    table_name="your_table_name",
    records=timestream_items,
    batch_size=100
)

```


### Query
```python
from aws_timestream_module.services.query import QueryService

query_service = QueryService(
    # Change the default config here
    # query_client=boto3.Session().client(
    #     'timestream-query',
    #     region_name="ap-southeast-2",
    # )
)

# Query items using SQL
sql = """
select * from example_table
"""
result = query_service.run_query(query_string=sql)
# output:
# [{'example_field_1': 'example_value', ...}]
```
