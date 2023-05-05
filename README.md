# Amazon Timestream Module

## Install this package
```pip install git+ssh://git@github.com/VoltaApp/aws-timestream-module.git@<branch_name>```

## Example usages
```python
from datetime import datetime
from aws_timestream_module.services.write import WriteService

write_service = WriteService()

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
    records=timestream_items
)

```