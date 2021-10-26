#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#


import csv
import io
import json
import pkgutil
from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple
from urllib.parse import urljoin

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.requests_native_auth import Oauth2Authenticator

"""
TODO: Most comments in this class are instructive and should be deleted after the source is implemented.

This file provides a stubbed example of how to use the Airbyte CDK to develop both a source connector which supports full refresh or and an
incremental syncs from an HTTP API.

The various TODOs are both implementation hints and steps - fulfilling all the TODOs should be sufficient to implement one basic and one incremental
stream from a source. This pattern is the same one used by Airbyte internally to implement connectors.

The approach here is not authoritative, and devs are free to use their own judgement.

There are additional required TODOs in the files within the integration_tests folder and the spec.json file.
"""


# Basic full refresh stream
class YoutubeAnalyticsStream(HttpStream, ABC):
    """
    TODO remove this comment

    This class represents a stream output by the connector.
    This is an abstract base class meant to contain all the common functionality at the API level e.g: the API base URL, pagination strategy,
    parsing responses etc..

    Each stream should extend this class (or another abstract subclass of it) to specify behavior unique to that stream.

    Typically for REST APIs each stream corresponds to a resource in the API. For example if the API
    contains the endpoints
        - GET v1/customers
        - GET v1/employees

    then you should have three classes:
    `class YoutubeAnalyticsStream(HttpStream, ABC)` which is the current class
    `class Customers(YoutubeAnalyticsStream)` contains behavior to pull data for customers using v1/customers
    `class Employees(YoutubeAnalyticsStream)` contains behavior to pull data for employees using v1/employees

    If some streams implement incremental sync, it is typical to create another class
    `class IncrementalYoutubeAnalyticsStream((YoutubeAnalyticsStream), ABC)` then have concrete stream implementations extend it. An example
    is provided below.

    See the reference docs for the full list of configurable options.
    """

    url_base = "https://youtubereporting.googleapis.com/v1/"

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        """
        TODO: Override this method to define a pagination strategy. If you will not be using pagination, no action is required - just return None.

        This method should return a Mapping (e.g: dict) containing whatever information required to make paginated requests. This dict is passed
        to most other methods in this class to help you form headers, request bodies, query params, etc..

        For example, if the API accepts a 'page' parameter to determine which page of the result to return, and a response from the API contains a
        'page' number, then this method should probably return a dict {'page': response.json()['page'] + 1} to increment the page count by 1.
        The request_params method should then read the input next_page_token and set the 'page' param to next_page_token['page'].

        :param response: the most recent response from the API
        :return If there is another page in the result, a mapping (e.g: dict) containing information needed to query the next page in the response.
                If there are no more pages in the result, return None.
        """
        return None

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        """
        TODO: Override this method to define any query parameters to be set. Remove this method if you don't need to define request params.
        Usually contains common params e.g. pagination size etc.
        """
        return {}

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        response_json = response.json()
        for report in response_json.get("reports", []):
            downloadUrl = report["downloadUrl"]
            response = self._session.get(downloadUrl)
            response.raise_for_status()
            fp = io.StringIO(response.text)
            reader = csv.DictReader(fp)
            for record in reader:
                yield record


class ChannelReport(YoutubeAnalyticsStream):
    name = None
    primary_key = None

    def __init__(self, name: str, dimensions: List[str], job_id: str, authenticator=None):
        self.name = name
        self.primary_key = dimensions
        self.job_id = job_id
        return super().__init__(authenticator)

    def create_job(self, name):
        request_json = {
            "name": "Airbyte reporting job",
            "reportTypeId": name,
        }
        url = urljoin(self.url_base, "jobs")
        response = self._session.post(url, json=request_json)
        response.raise_for_status()
        response_json = response.json()
        return response_json["id"]

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        if not self.job_id:
            self.job_id = self.create_job(self.name)
            self.logger.info(f"YouTube reporting job is created: '{self.job_id}'")
        return "jobs/{}/reports".format(self.job_id)


# Basic incremental stream
class IncrementalYoutubeAnalyticsStream(YoutubeAnalyticsStream, ABC):
    """
    TODO fill in details of this class to implement functionality related to incremental syncs for your connector.
         if you do not need to implement incremental sync for any streams, remove this class.
    """

    # TODO: Fill in to checkpoint stream reads after N records. This prevents re-reading of data if the stream fails for any reason.
    state_checkpoint_interval = None

    @property
    def cursor_field(self) -> str:
        """
        TODO
        Override to return the cursor field used by this stream e.g: an API entity might always use created_at as the cursor field. This is
        usually id or date based. This field's presence tells the framework this in an incremental stream. Required for incremental.

        :return str: The name of the cursor field.
        """
        return []

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        """
        Override to determine the latest state after reading the latest record. This typically compared the cursor_field from the latest record and
        the current state and picks the 'most' recent cursor. This is how a stream's state is determined. Required for incremental.
        """
        return {}


class Employees(IncrementalYoutubeAnalyticsStream):
    """
    TODO: Change class name to match the table/data source this stream corresponds to.
    """

    # TODO: Fill in the cursor_field. Required.
    cursor_field = "start_date"

    # TODO: Fill in the primary key. Required. This is usually a unique field in the stream, like an ID or a timestamp.
    primary_key = "employee_id"

    def path(self, **kwargs) -> str:
        """
        TODO: Override this method to define the path this stream corresponds to. E.g. if the url is https://example-api.com/v1/employees then this should
        return "single". Required.
        """
        return "employees"

    def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        """
        TODO: Optionally override this method to define this stream's slices. If slicing is not needed, delete this method.

        Slices control when state is saved. Specifically, state is saved after a slice has been fully read.
        This is useful if the API offers reads by groups or filters, and can be paired with the state object to make reads efficient. See the "concepts"
        section of the docs for more information.

        The function is called before reading any records in a stream. It returns an Iterable of dicts, each containing the
        necessary data to craft a request for a slice. The stream state is usually referenced to determine what slices need to be created.
        This means that data in a slice is usually closely related to a stream's cursor_field and stream_state.

        An HTTP request is made for each returned slice. The same slice can be accessed in the path, request_params and request_header functions to help
        craft that specific request.

        For example, if https://example-api.com/v1/employees offers a date query params that returns data for that particular day, one way to implement
        this would be to consult the stream state object for the last synced date, then return a slice containing each date from the last synced date
        till now. The request_params function would then grab the date from the stream_slice and make it part of the request by injecting it into
        the date query param.
        """
        raise NotImplementedError("Implement stream slices or delete this method!")


class SourceYoutubeAnalytics(AbstractSource):

    @staticmethod
    def get_authenticator(config):
        client_id = config["client_id"]
        client_secret = config["client_secret"]
        refresh_token = config["refresh_token"]

        return Oauth2Authenticator(
            token_refresh_endpoint="https://oauth2.googleapis.com/token",
            client_id=client_id,
            client_secret=client_secret,
            refresh_token=refresh_token,
            scopes=["https://www.googleapis.com/auth/yt-analytics-monetary.readonly"],
        )

    def get_jobs(self, authenticator):
        headers = authenticator.get_auth_header()
        url = urljoin(YoutubeAnalyticsStream.url_base, "jobs")
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json().get("jobs", {})

    def check_connection(self, logger, config) -> Tuple[bool, any]:
        authenticator = self.get_authenticator(config)

        try:
            self.get_jobs(authenticator)
        except Exception as e:
            return False, str(e)

        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        authenticator = self.get_authenticator(config)
        jobs = self.get_jobs(authenticator)
        report_to_job_id = {j["reportTypeId"]: j["id"] for j in jobs}

        channel_reports = json.loads(pkgutil.get_data("source_youtube_analytics", "defaults/channel_reports.json"))

        streams = []
        for channel_report in channel_reports:
            stream_name = channel_report["id"]
            dimensions = channel_report["dimensions"]
            job_id = report_to_job_id.get(stream_name)
            streams.append(ChannelReport(stream_name, dimensions, job_id, authenticator))
        return streams
