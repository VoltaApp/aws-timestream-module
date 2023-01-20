from typing import List

from aws_timestream_module.utils.abstracts.utils import AbstractUtils


class BillReaderUtils(AbstractUtils):

    def __init__(
        self,
        measure_name: str,
        is_from_bill: bool = False,
    ) -> None:
        '''An implement of AbstractUtils class for Bill Readers

        :param measure_name: MeasureName in AWS Timestream table
        :param is_from_bill: True if init an object extract from a bill. Default: False - User enter manually

        Example::

            # The code below shows an example of how to instantiate this type.
            # The values are placeholders you should change.
            from utils.implements.bill_readers_util import BillReaderUtils

            utils = BillReaderUtils(
                measure_name="energy_measure",
            )
        '''
        super().__init__(
            measure_name,
        )
        self.time_field = "issue_time"
        self.dimension_fields = [
            "username",
            "nmi",
            "gmt",
        ]
        self.based_measure_value_fields = [
            "unit_of_measure",
            "is_from_bill",
        ]
        self.user_manual_input_measure_value_fields = [
            "manual_usage",
        ]
        self.extracting_bill_measure_value_fields = [
            "bill_readers",
            "read_type",
            "billed_usage",
            "start_reading_usage",
            "end_reading_usage",
            "billed_cost",
            "discount",
            "due_date",
            "billing_days",
            "tax_invoice",
            "charging_start_date",
            "charging_end_date",
            "next_reading_date",
            "customer_name",
            "customer_address",
            "customer_email",
            "customer_retailer",
            "customer_plan",
            "bill_image",
        ]
        self.is_from_bill = is_from_bill

    def get_records_with_multi_type(
        self,
        items: List[dict],
    ) -> List[dict]:
        '''An implement get_records_with_multi_type from AbstractUtils class. Use this one to prepare data for inserting items to AWS Timestream

        :param items: the list of dicts to convert to AWS Timestream items

        Example::

            # The code below shows an example of how to instantiate this type.
            # The values are placeholders you should change.
            import json
            from datetime import datetime, date
            from utils.implements.bill_readers_util import BillReaderUtils

            bill_readers = [
                {
                    "billed_usage": 709.97,
                    "billed_cost": 215.07,
                    "unit_of_measure": "kWh",
                    "due_date": "2022-09-12T00:00:00",
                    "billing_days": 31,
                    "charging_start_date": "2022-07-29T00:00:00",
                    "charging_end_date": "2022-08-28T00:00:00",
                    "start_reading_usage": 17135.23,
                    "end_reading_usage": 17845.2,
                    "read_type": "A"
                }
            ]
            items = [
                {
                    "issue_time": datetime.now(),
                    "username": "any_username",
                    "nmi": "1234567890",
                    "unit_of_measure": "kWh",
                    ## manually
                    ## "manual_usage": 1234567890,
                    # from bill
                    "bill_readers": json.dumps(bill_readers),
                    "read_type": "A",
                    "billed_usage": 709.97,
                    "start_reading_usage": 17135.23,
                    "end_reading_usage": 17845.2,
                    "billed_cost": 215.07,
                    "discount": 5,  # optional
                    "gmt": "+11",
                    "charging_start_date": "2022-07-29T00:00:00",
                    "charging_end_date": "2022-08-28T00:00:00",
                    "next_reading_date": "2022-08-29T00:00:00",  # optional
                    "due_date": "2022-09-12T00:00:00",
                    "billing_days": 31,
                    "tax_invoice": "any tax invoice",
                    "customer_name": "any customer name",
                    "customer_address": "any address",
                    "customer_email": "example@email.com",  # optional
                    "customer_retailer": "any retailer",
                    "customer_plan": "any plan",  # optional
                    "bill_image": json.dumps({
                        "s3_region": "region",
                        "s3_bucket": "bucket",
                        "s3_key": "key"
                    }),
                }
            ]
            utils = BillReaderUtils(
                measure_name="energy_measure",
                is_from_bill=True,
            )
            timestream_items = utils.get_records_with_multi_type(
                items
            )
            # output:
            timestream_items = [{'MeasureName': 'energy_measure', 'Dimensions': [{'Name': 'username', 'Value': 'any_username'}, {'Name': 'nmi', 'Value': '1234567890'}], 'MeasureValueType': 'MULTI', 'Time': '1673461870194', 'MeasureValues': [{'Name': 'unit_of_measure', 'Value': 'kWh', 'Type': 'VARCHAR'}, {'Name': 'bill_readers', 'Value': '[{"read_type": "E", "billed_usage": 123, "start_reading_usage": 0, "end_reading_usage": 123, "billed_cost": 500.5, "discount": 5}]', 'Type': 'VARCHAR'}, {'Name': 'read_type', 'Value': 'E', 'Type': 'VARCHAR'}, {'Name': 'billed_usage', 'Value': '123', 'Type': 'BIGINT'}, {'Name': 'start_reading_usage', 'Value': '0', 'Type': 'BIGINT'}, {'Name': 'end_reading_usage', 'Value': '123', 'Type': 'BIGINT'}, {'Name': 'billed_cost', 'Value': '500.5', 'Type': 'DOUBLE'}, {'Name': 'discount', 'Value': '5', 'Type': 'BIGINT'}, {'Name': 'charging_start_date', 'Value': '2022-12-01', 'Type': 'VARCHAR'}, {'Name': 'charging_end_date', 'Value': '2022-01-01', 'Type': 'VARCHAR'}, {'Name': 'next_reading_date', 'Value': '2022-01-01', 'Type': 'VARCHAR'}, {'Name': 'customer_name', 'Value': 'any customer name', 'Type': 'VARCHAR'}, {'Name': 'customer_address', 'Value': 'any address', 'Type': 'VARCHAR'}, {'Name': 'customer_email', 'Value': 'example@email.com', 'Type': 'VARCHAR'}, {'Name': 'customer_retailer', 'Value': 'any retailer', 'Type': 'VARCHAR'}, {'Name': 'customer_plan', 'Value': 'any plan', 'Type': 'VARCHAR'}, {'Name': 'bill_image', 'Value': '{"s3_region": "region", "s3_bucket": "bucket", "s3_key": "key"}', 'Type': 'VARCHAR'}]}]
        '''
        timestream_records = []
        for item in items:
            based_record = self.get_based_record_with_multi_type(
                item,
            )
            measure_value_fields = self.based_measure_value_fields
            item.update({"is_from_bill": self.is_from_bill})
            if self.is_from_bill:
                measure_value_fields.extend(
                    self.extracting_bill_measure_value_fields
                )
            else:
                measure_value_fields.extend(
                    self.user_manual_input_measure_value_fields
                )
            measure_values = self.get_measure_values(
                item,
                measure_value_fields
            )
            timestream_record = based_record
            timestream_record.update({
                "Time": self.convert_datetime_to_timeseries(
                    any_datetime=item.get(self.time_field)
                ),
                "MeasureValues": measure_values
            })
            timestream_records.append(timestream_record)
        return timestream_records
