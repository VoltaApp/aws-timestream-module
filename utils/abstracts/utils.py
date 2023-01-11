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
    def convert_datetime_to_timestream_time(
        any_datetime: datetime,
    ) -> str:
        '''Convert datetime object to `Time` data type in AWS Timestream
        :param any_datetime: the datetime object to convert
        Example::
            # The code below shows an example of how to instantiate this type.
            # The values are placeholders you should change.
            from utils.abstracts.utils import AbstractUtils

            any_datetime: datetime
            timeseries = AbstractUtils.convert_datetime_to_timestream_time(any_datetime)
        '''
        if isinstance(any_datetime, datetime):
            result = str(int(any_datetime.timestamp()*1000))
            return result
        raise ValueError(f"any_datetime only accepts datetime type: {any_datetime}")

    @staticmethod
    def get_dimensions_from_dict(
        any_dict: dict,
    ) -> List[dict]:
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
        if isinstance(value, str):
            return "VARCHAR"
        if isinstance(value, float):
            return "DOUBLE"
        if isinstance(value, int):
            return "BIGINT"
        if isinstance(value, bool):
            return "BOOLEAN"
        if isinstance(value, float):
            return "DOUBLE"
        raise ValueError(f"Not supported type: {type(value)}")

    def get_measure_value(
        self,
        name: str,
        value: Any,
    ) -> dict:
        return {
            'Name': name,
            'Value': str(value),
            'Type': self.__cast_value(value),
        }

    def get_based_record_with_multi_type(
        self,
        item: dict
    ) -> dict:
        based_record = {
            "MeasureName": self.measure_name,
            "Dimensions": self.get_dimensions_from_dict({
                key: item.get(key)
                    for key in self.dimension_fields
            }),
            "MeasureValueType": "MULTI"
        }
        return based_record

    def get_measure_values(
        self,
        item: dict,
        measure_value_fields: List[str] = []
    ) -> List[dict]:
        fields = measure_value_fields or self.based_measure_value_fields
        for key in fields:
            self.get_measure_value(
                key,
                item.get(key),
            )
        based_measure_values = [
            self.get_measure_value(
                key,
                item.get(key),
            ) for key in fields
        ]
        return based_measure_values