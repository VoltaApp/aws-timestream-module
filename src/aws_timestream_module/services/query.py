#!/usr/bin/python
import sys
import traceback
import time
from aws_timestream_module.aws.timestream import (
    query_client as _query_client,
)


class QueryService:

    def __init__(self, query_client=_query_client):
        self._query_client = query_client
        self.paginator = query_client.get_paginator('query')

    def run_query(self, query_string):
        try:
            results = []
            page_iterator = self.paginator.paginate(QueryString=query_string)
            for page in page_iterator:
                results.extend(self.__parse_query_result(page))
            return results
        except Exception as err:
            print("Exception while running query:", err)
            traceback.print_exc(file=sys.stderr)
            return []

    def __parse_query_result(self, query_result):
        column_info = query_result['ColumnInfo']
        result = []
        for row in query_result['Rows']:
            result.append(self.__parse_row(column_info, row))
        return result

    def __parse_row(self, column_info, row):
        data = row['Data']
        row_output = {}
        for j in range(len(data)):
            info = column_info[j]
            datum = data[j]
            row_output.update(self.__parse_datum(info, datum))

        return row_output

    def __parse_datum(self, info, datum):
        if datum.get('NullValue', False):
            return {
                self.__parse_column_name(info): ''
            }

        column_type = info['Type']

        # If the column is of TimeSeries Type
        if 'TimeSeriesMeasureValueColumnInfo' in column_type:
            return self.__parse_time_series(info, datum)

        # If the column is of Array Type
        elif 'ArrayColumnInfo' in column_type:
            array_values = datum['ArrayValue']
            return "%s=%s" % (
                info['Name'],
                self.__parse_array(
                    info['Type']['ArrayColumnInfo'], array_values
                )
            )

        # If the column is of Row Type
        elif 'RowColumnInfo' in column_type:
            row_column_info = info['Type']['RowColumnInfo']
            row_values = datum['RowValue']
            return self.__parse_row(row_column_info, row_values)

        # If the column is of Scalar Type
        else:
            return {
                self.__parse_column_name(info): datum['ScalarValue']
            }

    def __parse_time_series(self, info, datum):
        time_series_output = [
            f"""
            {{time={data_point['Time']},
            value={
                self.__parse_datum(
                    info['Type']['TimeSeriesMeasureValueColumnInfo'],
                    data_point['Value'])
                }
            }}"""
            for data_point in datum['TimeSeriesValue']
        ]

        return f"[{time_series_output}]"

    def __parse_column_name(self, info):
        if 'Name' in info:
            return info['Name']
        else:
            return ""

    def __parse_array(self, array_column_info, array_values):
        array_output = [
            f"{self.__parse_datum(array_column_info, datum)}"
            for datum in array_values
        ]

        return f"[{array_output}]"

    def run_query_with_multiple_pages(self, limit=None):
        query_with_limit = self.SELECT_ALL
        if limit is not None:
            query_with_limit += " LIMIT " + str(limit)
        print("Starting query with multiple pages : " + query_with_limit)
        self.run_query(query_with_limit)

    def cancel_query(self):
        print("Starting query: " + self.SELECT_ALL)
        result = self._query_client.query(QueryString=self.SELECT_ALL)
        print("Cancelling query: " + self.SELECT_ALL)
        try:
            self._query_client.cancel_query(QueryId=result['QueryId'])
            print("Query has been successfully cancelled")
        except Exception as err:
            print("Cancelling query failed:", err)

    @staticmethod
    def current_milli_time():
        return str(int(round(time.time() * 1000)))
