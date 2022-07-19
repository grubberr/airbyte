#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


import json
from datetime import datetime
from typing import Dict, Generator

from airbyte_cdk.logger import AirbyteLogger
from airbyte_cdk.models import (
    AirbyteCatalog,
    AirbyteConnectionStatus,
    AirbyteMessage,
    AirbyteRecordMessage,
    AirbyteStream,
    ConfiguredAirbyteCatalog,
    Status,
    Type,
)
from airbyte_cdk.sources import Source


class SourceAllTypes(Source):
    def check(self, logger: AirbyteLogger, config: json) -> AirbyteConnectionStatus:
        """
        Tests if the input configuration can be used to successfully connect to the integration
            e.g: if a provided Stripe API token can be used to connect to the Stripe API.

        :param logger: Logging object to display debug/info/error to the logs
            (logs will not be accessible via airbyte UI if they are not passed to this logger)
        :param config: Json object containing the configuration of this source, content of this json is as specified in
        the properties of the spec.yaml file

        :return: AirbyteConnectionStatus indicating a Success or Failure
        """
        try:
            # Not Implemented

            return AirbyteConnectionStatus(status=Status.SUCCEEDED)
        except Exception as e:
            return AirbyteConnectionStatus(status=Status.FAILED, message=f"An exception occurred: {str(e)}")

    def discover(self, logger: AirbyteLogger, config: json) -> AirbyteCatalog:
        """
        Returns an AirbyteCatalog representing the available streams and fields in this integration.
        For example, given valid credentials to a Postgres database,
        returns an Airbyte catalog where each postgres table is a stream, and each table column is a field.

        :param logger: Logging object to display debug/info/error to the logs
            (logs will not be accessible via airbyte UI if they are not passed to this logger)
        :param config: Json object containing the configuration of this source, content of this json is as specified in
        the properties of the spec.yaml file

        :return: AirbyteCatalog is an object describing a list of all available streams in this source.
            A stream is an AirbyteStream object that includes:
            - its stream name (or table name in the case of Postgres)
            - json_schema providing the specifications of expected schema for this stream (a list of columns described
            by their names and types)
        """
        streams = []
        stream_name = "TableName"
        json_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "type_string": {"type": "string"},
                "type_number": {"type": "number"},
                "type_integer": {"type": "integer"},
                "type_boolean": {"type": "boolean"},
                "type_object": {
                    "type": "object",
                    "properties": {
                        "type_string": {"type": "string"},
                        "type_number": {"type": "number"},
                        "type_integer": {"type": "integer"},
                        "type_boolean": {"type": "boolean"},
                    },
                },
                "type_array_of_strings": {
                    "type": "array",
                    "items": {
                        "type": "string",
                    },
                },
                "type_date": {"type": "string", "format": "date"},
                "type_datetime_with_timezone": {"type": "string", "format": "date-time", "airbyte_type": "timestamp_with_timezone"},
                "type_datetime_without_timezone": {"type": "string", "format": "date-time", "airbyte_type": "timestamp_without_timezone"},
                "type_big_integer": {"type": "string", "airbyte_type": "big_integer"},
                "type_big_number": {"type": "string", "airbyte_type": "big_number"},
                "type_binary": {"type": "string", "contentEncoding": "base64"},
                "type_object_of_datetime_with_timezone": {
                    "type": "object",
                    "properties": {
                        "datetime_1": {"type": "string", "format": "date-time", "airbyte_type": "timestamp_with_timezone"},
                        "datetime_2": {"type": "string", "format": "date-time", "airbyte_type": "timestamp_with_timezone"},
                        "datetime_3": {"type": "string", "format": "date-time", "airbyte_type": "timestamp_with_timezone"},
                        "datetime_4": {"type": "string", "format": "date-time", "airbyte_type": "timestamp_with_timezone"},
                        "datetime_5": {"type": "string", "format": "date-time", "airbyte_type": "timestamp_with_timezone"},
                        "datetime_6": {"type": "string", "format": "date-time", "airbyte_type": "timestamp_with_timezone"},
                        "datetime_7": {"type": "string", "format": "date-time", "airbyte_type": "timestamp_with_timezone"},
                        "datetime_8": {"type": "string", "format": "date-time", "airbyte_type": "timestamp_with_timezone"},
                    },
                },
                "type_object_of_datetime_without_timezone": {
                    "type": "object",
                    "properties": {
                        "datetime_1": {"type": "string", "format": "date-time", "airbyte_type": "timestamp_without_timezone"},
                        "datetime_2": {"type": "string", "format": "date-time", "airbyte_type": "timestamp_without_timezone"},
                    },
                },
                "custom_fields": {
                    "type": ["null", "array"],
                    "items": {
                        "type": ["null", "object"],
                        "properties": {"id": {"type": ["null", "integer"]}, "value": {"type": ["null", "string"]}},
                    },
                },
            },
        }

        # Not Implemented

        streams.append(AirbyteStream(name=stream_name, json_schema=json_schema))
        return AirbyteCatalog(streams=streams)

    def read(
        self, logger: AirbyteLogger, config: json, catalog: ConfiguredAirbyteCatalog, state: Dict[str, any]
    ) -> Generator[AirbyteMessage, None, None]:
        """
        Returns a generator of the AirbyteMessages generated by reading the source with the given configuration,
        catalog, and state.

        :param logger: Logging object to display debug/info/error to the logs
            (logs will not be accessible via airbyte UI if they are not passed to this logger)
        :param config: Json object containing the configuration of this source, content of this json is as specified in
            the properties of the spec.yaml file
        :param catalog: The input catalog is a ConfiguredAirbyteCatalog which is almost the same as AirbyteCatalog
            returned by discover(), but
        in addition, it's been configured in the UI! For each particular stream and field, there may have been provided
        with extra modifications such as: filtering streams and/or columns out, renaming some entities, etc
        :param state: When a Airbyte reads data from a source, it might need to keep a checkpoint cursor to resume
            replication in the future from that saved checkpoint.
            This is the object that is provided with state from previous runs and avoid replicating the entire set of
            data everytime.

        :return: A generator that produces a stream of AirbyteRecordMessage contained in AirbyteMessage object.
        """
        stream_name = "TableName"
        data = {
            "type_string": "string",
            "type_number": 10.10,
            "type_integer": 10,
            "type_boolean": True,
            "type_object": {
                "type_string": "sub-string",
                "type_number": 10.10,
                "type_integer": 10,
                "type_boolean": False,
            },
            "type_array_of_strings": ["string1", "string2", "string3"],
            "type_date": "2021-01-23",
            "type_datetime_with_timezone": "2022-11-22T01:23:45+05:00",
            "type_datetime_without_timezone": "2022-11-22T01:23:45",
            "type_big_integer": "123141241234124123141241234124123141241234124123141241234124123141241234124",
            "type_big_number": "1000000000000000000000000000000000.1234",
            "type_binary": "kFB5dGhvbiBpcyBmdW6Q",
            "type_object_of_datetime_with_timezone": {
                "datetime_1": "2022-11-22T01:23:45+0530",
                "datetime_2": "2022-11-22T01:23:45-0530",
                "datetime_3": "2022-11-22T01:23:45+05",
                "datetime_4": "2022-11-22T01:23:45-05",
                "datetime_5": "2022-11-22T01:23:45.9999999+0130",
                "datetime_6": "2022-11-22T01:23:45.9999999-0130",
                "datetime_7": "2022-11-22T01:23:45.9999999-01",
                "datetime_8": "2022-11-22T01:23:45.9999999+01",
            },
            "type_object_of_datetime_without_timezone": {
                "datetime_1": "2022-11-22T01:23:45",
                "datetime_2": "2022-11-22T01:23:45.9999999",
            },
            "custom_fields": [
                {"id": 360023382300, "value": None},
                {"id": 360004841380, "value": "customer_tickets"},
                {"id": 360022469240, "value": "5"},
                {"id": 360023712840, "value": False},
            ],
        }

        yield AirbyteMessage(
            type=Type.RECORD,
            record=AirbyteRecordMessage(stream=stream_name, data=data, emitted_at=int(datetime.now().timestamp()) * 1000),
        )
