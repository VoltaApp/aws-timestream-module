from abc import (
    ABC,
    abstractmethod,
)
from typing import (
    Any,
    Iterable,
    List,
)
from datetime import datetime


class AbstractUtils(ABC):

    def __init__(
        self,
        measure_name: str,
    ) -> None:
        '''Abstract Utils class

        :param measure_name: MeasureName in AWS Timestream table

        Example::

            # The code below shows an example of how to instantiate this type.
            # The values are placeholders you should change.
            from utils.abstracts.utils import AbstractUtils

            utils = AbstractUtils(
                measure_name="energy_measure",
            )
        '''
        self.measure_name = measure_name
        self.dimension_fields = []
        self.based_measure_value_fields = []

    @abstractmethod
    def get_records_with_multi_type(
        self,
        items: List[dict],
    ) -> List[dict]:
        '''An Abstract method in order to convert `dict items` to `AWS Timestream items`'''
        pass

    @staticmethod
    def batch(
        iterable: Iterable,
        n: int = 1,
    ):
        '''Divide batch into sub batch

        :param iterable: the batch iterable you want to divide to smaller parts
        :param n: the len of items in each part

        Example::

            # The code below shows an example of how to instantiate this type.
            # The values are placeholders you should change.
            from utils.abstracts.utils import AbstractUtils

            records: List
            for sub_records in AbstractUtils.batch(records, 100):
                # do something with sub_records
        '''
        len_iterable = len(iterable)
        for ndx in range(0, len_iterable, n):
            yield iterable[ndx:min(ndx + n, len_iterable)]

    @staticmethod
    def convert_datetime_to_timeseries(
        any_datetime: datetime,
    ) -> str:
        '''Convert datetime to `Time` data type in AWS Timestream

        :param any_datetime: the datetime object to convert

        Example::

            # The code below shows an example of how to instantiate this type.
            # The values are placeholders you should change.
            from utils.abstracts.utils import AbstractUtils

            any_datetime: datetime
            timeseries = AbstractUtils.convert_datetime_to_timeseries(any_datetime)
        '''
        if isinstance(any_datetime, datetime):
            result = str(int(any_datetime.timestamp()*1000))
            return result
        raise ValueError(f"any_datetime only accepts datetime type: {any_datetime}")

    @staticmethod
    def get_dimensions_from_dict(
        any_dict: dict,
    ) -> List[dict]:
        '''Convert dict to Dimensions in AWS Timestream

        :param any_dict: the dict object to convert

        Example::

            # The code below shows an example of how to instantiate this type.
            # The values are placeholders you should change.
            from utils.abstracts.utils import AbstractUtils

            any_dict: dict
            dimensions = AbstractUtils.get_dimensions_from_dict(any_dict)
        '''
        dimensions = []
        keys = any_dict.keys()
        values = any_dict.values()
        for key, value in zip(keys, values):
            dimensions.append(
                {
                    'Name': key,
                    'Value': value
                }
            )
        return dimensions

    def __cast_value(
        self,
        value: Any
    ) -> str:
        '''Return AWS Timestream data type from a python instance

        :param value: a python instance

        Example::

            # The code below shows an example of how to instantiate this type.
            # The values are placeholders you should change.
            from utils.abstracts.utils import AbstractUtils
            from typing import Any

            utils: AbstractUtils
            value: Any
            timestream_type = utils.__cast_value(value)
        '''
        if isinstance(value, str):
            return "VARCHAR"
        if isinstance(value, float):
            return "DOUBLE"
        if isinstance(value, bool):
            return "BOOLEAN"
        if isinstance(value, int):
            return "BIGINT"
        raise ValueError(f"Not supported type: {type(value)}")

    def get_measure_value(
        self,
        name: str,
        value: Any,
    ) -> dict:
        '''Return a measure_value in AWS Timestream

        :param name: the field name
        :param value: value of the field

        Example::

            # The code below shows an example of how to instantiate this type.
            # The values are placeholders you should change.
            from utils.abstracts.utils import AbstractUtils

            utils: AbstractUtils
            measure_value = utils.get_measure_value(
                name="any_field",
                value=100
            )
        '''
        return {
            'Name': name,
            'Value': str(value),
            'Type': self.__cast_value(value),
        }

    def get_based_record_with_multi_type(
        self,
        any_item: dict
    ) -> dict:
        '''Return a AWS Timestream based record with multi type before insert into Timestream table

        :param any_item: the item

        Example::

            # The code below shows an example of how to instantiate this type.
            # The values are placeholders you should change.
            from utils.abstracts.utils import AbstractUtils

            utils: AbstractUtils
            item: dict
            measure_value = utils.get_based_record_with_multi_type(
                any_item=item
            )
        '''
        based_record = {
            "MeasureName": self.measure_name,
            "Dimensions": self.get_dimensions_from_dict({
                key: any_item.get(key)
                    for key in self.dimension_fields
            }),
            "MeasureValueType": "MULTI"
        }
        return based_record

    def get_measure_values(
        self,
        any_item: dict,
        measure_value_fields: List[str] = []
    ) -> List[dict]:
        '''Return the AWS Timestream measure values

        :param any_item: will map value from this item to measure values
        :param measure_value_fields: list of measure field names

        Example::

            # The code below shows an example of how to instantiate this type.
            # The values are placeholders you should change.
            from utils.abstracts.utils import AbstractUtils

            utils: AbstractUtils
            item: dict
            measure_value_fields: List[str] = [
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
            measure_values = utils.get_measure_values(
                any_item=item
                measure_value_fields=measure_value_fields
            )
        '''
        fields = measure_value_fields or self.based_measure_value_fields
        based_measure_values = [
            self.get_measure_value(
                key,
                any_item.get(key, "None"),
            ) for key in fields
        ]
        return based_measure_values
