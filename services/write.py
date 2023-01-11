from aws.timestream import write_client
from core.config import config
from utils.abstracts.utils import AbstractUtils


class WriteService():
    def __init__(
        self,
        write_client=write_client,
    ) -> None:
        self.write_client = write_client
        self.common_attrs = {}

    def write_records(self, records: list):
        try:
            for sub_records in AbstractUtils.batch(records, 100):
                result = self.write_client.write_records(
                    DatabaseName=config.db_name,
                    TableName=config.table_name,
                    Records=sub_records,
                    CommonAttributes=self.common_attrs
                )
                print("WriteRecords Status: ["
                + f"{result['ResponseMetadata']['HTTPStatusCode']}]")
        except Exception as err:
            print(err.response)
            print(err.response["Error"])
