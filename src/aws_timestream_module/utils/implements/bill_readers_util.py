from typing import (
    List,
)
from aws_timestream_module.utils.abstracts.utils import (
    AbstractUtils,
)


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
        self.dimension_fields = [
            "username",
            "nmi"
        ]
        self.based_measure_value_fields = [
            "unit_of_measure",
        ]
        self.user_manual_input_measure_value_fields = [
            "manual_usage",
            "issue_time"
        ]
        self.extracting_bill_measure_value_fields = [
            "bill_readers",
            "read_type",
            "billed_usage",
            "start_reading_usage",
            "end_reading_usage",
            "billed_cost",
            "discount",
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
            from utils.implements.bill_readers_util import BillReaderUtils

            items: List[dict]
            utils = BillReaderUtils(
                measure_name="energy_measure",
            )
            timestream_items = utils.get_records_with_multi_type(
                items
            )
        '''
        timestream_records = []
        for item in items:
            based_record = self.get_based_record_with_multi_type(
                item,
            )
            measure_value_fields = self.based_measure_value_fields
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
                    any_datetime=item.get('issue_date')
                ),
                "MeasureValues": measure_values
            })
            timestream_records.append(timestream_record)
        return timestream_records
