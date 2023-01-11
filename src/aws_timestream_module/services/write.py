from aws_timestream_module.aws.timestream import (
    write_client as _write_client,
)
from aws_timestream_module.core.config import (
    config as _config,
)
from aws_timestream_module.utils.abstracts.utils import (
    AbstractUtils,
)


class WriteService():
    def __init__(
        self,
        write_client=_write_client,
    ) -> None:
        self._write_client = write_client
        self.common_attrs = {}

    def write_records(self, records: list):
        try:
            for sub_records in AbstractUtils.batch(records, 100):
                result = self._write_client.write_records(
                    DatabaseName=_config.db_name,
                    TableName=_config.table_name,
                    Records=sub_records,
                    CommonAttributes=self.common_attrs
                )
                print("WriteRecords Status: ["
                + f"{result['ResponseMetadata']['HTTPStatusCode']}]")
        except Exception as err:
            print(err.response)
            print(err.response["Error"])
