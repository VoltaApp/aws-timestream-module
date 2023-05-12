from typing import List

import boto3
from botocore.config import Config

from aws_timestream_module.utils.timestream_utils import TimestreamUtils


class WriteService():

    boto3_session = boto3.Session()

    def __init__(
        self,
        timestream_utils = TimestreamUtils(),
        common_attrs: dict = {},
        write_client=None,
    ) -> None:
        self._timestream_utils = timestream_utils
        self.common_attrs = common_attrs
        # Recommended Timestream write client SDK configuration:
        #  - Set SDK retry count to 10
        #  - Use SDK DEFAULT_BACKOFF_STRATEGY
        #  - Set RequestTimeout to 20 seconds
        #  - Set max connections to 5000 or higher
        self._write_client = write_client or self.boto3_session.client(
            "timestream-write",
            region_name="ap-southeast-2",
            config=Config(
                read_timeout=20,
                max_pool_connections=5000,
                retries={"max_attempts": 10}
            )
        )

    def get_records_with_multi_type(
        self,
        measure_name: str,
        time_field: str,
        dimension_fields: List[str],
        measure_value_fields: List[str],
        items: List[dict],
    ) -> List[dict]:
        '''Prepare data before inserting to AWS Timestream

        :param measure_name: the name of the measure
        :param time_field: the name of the time field
        :param dimension_fields: the list of dimension fields
        :param measure_value_fields: the list of measure value fields
        :param items: the list of dicts to convert to AWS Timestream items

        Example::

            # The code below shows an example of how to instantiate this type.
            # The values are placeholders you should change.

            write_service: WriteService
            ts_items = write_service.get_records_with_multi_type(
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
        '''
        timestream_records = [
            {
                **self._timestream_utils.get_based_record_with_multi_type(
                    measure_name, dimension_fields, item
                ),
                "Time": TimestreamUtils.convert_datetime_to_timeseries(
                    any_datetime=item.get(time_field)
                ),
                "MeasureValues": self._timestream_utils.get_measure_values(
                    item, measure_value_fields
                )
            }
            for item in items
        ]
        return timestream_records

    def write_records(
        self,
        database_name: str,
        table_name: str,
        records: list,
        batch_size: 100
    ) -> None:
        try:
            for sub_records in TimestreamUtils.batch(records, batch_size):
                print({"write_records": sub_records})
                result = self._write_client.write_records(
                    DatabaseName=database_name,
                    TableName=table_name,
                    Records=sub_records,
                    CommonAttributes=self.common_attrs
                )
                print("WriteRecords Status: ["
                + f"{result['ResponseMetadata']['HTTPStatusCode']}]")
        except Exception as err:
            print(err.response)
            print(err.response["Error"])
