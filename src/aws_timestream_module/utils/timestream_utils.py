import json
from datetime import datetime
from typing import Any, Iterable, List, Union


class TimestreamUtils():

    def __init__(self) -> None:
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

            records: List
            for sub_records in TimestreamUtils.batch(records, 100):
                # do something with sub_records
        '''
        len_iterable = len(iterable)
        for ndx in range(0, len_iterable, n):
            yield iterable[ndx:min(ndx + n, len_iterable)]

    @staticmethod
    def convert_datetime_to_timeseries(
        any_datetime: Union[datetime, str],
    ) -> str:
        '''Convert datetime to `Time` data type in AWS Timestream

        :param any_datetime: the datetime object to convert

        Example::

            # The code below shows an example of how to instantiate this type.
            # The values are placeholders you should change.

            any_datetime: datetime
            timeseries = TimestreamUtils.convert_datetime_to_timeseries(any_datetime)
        '''
        try:
            datetime_input = any_datetime
            if isinstance(datetime_input, str):
                datetime_input = datetime.fromisoformat(datetime_input)
            result = str(int(datetime_input.timestamp()*1000))
            return result
        except ValueError:
            raise ValueError(f"any_datetime only accepts datetime format: {any_datetime}")

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

            utils: TimestreamUtils
            measure_value = utils.get_measure_value(
                name="any_field",
                value=100
            )
            # output
            # {'Type': 'BIGINT', 'Name': 'any_field', 'Value': 100}
        '''
        casted_value, timestream_type = self.__cast_value(value)
        return {
            'Name': name,
            'Value': casted_value,
            'Type': timestream_type,
        }

    def get_based_record_with_multi_type(
        self,
        measure_name: str,
        dimension_fields: List[str],
        any_item: dict
    ) -> dict:
        '''Return a AWS Timestream based record with multi type before insert into Timestream table

        :param any_item: the item

        Example::

            # The code below shows an example of how to instantiate this type.
            # The values are placeholders you should change.

            utils: TimestreamUtils
            any_item = {"any_field": "any_value"}
            measure_value = utils.get_based_record_with_multi_type(
                measure_name="any_measure_name",
                dimension_fields=["any_field"],
                any_item=any_item
            )
            # output
            # { "MeasureName": any_measure_name, "Dimensions": [{"Name": "any_field", "Value": "any_value"}]), "MeasureValueType": "MULTI"}
        '''
        based_record = {
            "MeasureName": measure_name,
            "Dimensions": self.__get_dimensions_from_dict({
                key: any_item.get(key)
                    for key in dimension_fields
            }),
            "MeasureValueType": "MULTI"
        }
        return based_record

    def get_measure_values(
        self,
        any_item: dict,
        measure_value_fields: List[str]
    ) -> List[dict]:
        '''Return the AWS Timestream measure values

        :param any_item: will map value from this item to measure values
        :param measure_value_fields: list of measure field names

        Example::

            # The code below shows an example of how to instantiate this type.
            # The values are placeholders you should change.
            from utils.abstracts.utils import TimestreamUtils

            utils: TimestreamUtils
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
        based_measure_values = [
            self.get_measure_value(
                key,
                any_item.get(key, "None"),
            ) for key in measure_value_fields
        ]
        return based_measure_values

    def __get_dimensions_from_dict(
        self,
        any_dict: dict,
    ) -> List[dict]:
        '''Convert dict to Dimensions in AWS Timestream

        :param any_dict: the dict object to convert

        Example::

            # The code below shows an example of how to instantiate this type.
            # The values are placeholders you should change.

            any_dict: dict
            dimensions = TimestreamUtils.get_dimensions_from_dict(any_dict)
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
    ) -> Union[Any, str]:
        '''Return AWS Timestream data type from a python instance

        :param value: a python instance

        Example::

            # The code below shows an example of how to instantiate this type.
            # The values are placeholders you should change.
            from utils.abstracts.utils import TimestreamUtils
            from typing import Any

            utils: TimestreamUtils
            value: Any
            casted_value, timestream_type = utils.__cast_value(value)
        '''
        casted_value = str(value)
        if isinstance(value, (list, dict)):
            casted_value = json.dumps(value)
            return casted_value, "VARCHAR"
        if isinstance(value, str):
            return casted_value, "VARCHAR"
        if isinstance(value, float):
            return casted_value, "DOUBLE"
        if isinstance(value, bool):
            return casted_value, "BOOLEAN"
        if isinstance(value, int):
            return casted_value, "BIGINT"
        raise ValueError(f"Not supported type: {type(value)}")
