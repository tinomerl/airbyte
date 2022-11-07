#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


from abc import ABC
from datetime import datetime
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import pendulum
import requests
from airbyte_cdk import AirbyteLogger
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream, IncrementalMixin
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth import Oauth2Authenticator, TokenAuthenticator


class LinkedinPagesStream(HttpStream, ABC):

    url_base = "https://api.linkedin.com/v2/"
    primary_key = ""

    def __init__(self, config, **kwargs):
        super().__init__(authenticator=config.get("authenticator"))
        self.config = config


    @property
    def org(self):
        """Property to return the user Organization Id from input"""
        return self.config.get("org_id")

    def path(self, **kwargs) -> str:
        """Returns the API endpoint path for stream, from `endpoint` class attribute."""
        return self.endpoint

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        content = response.json()
        paging = content.get("paging")
        if paging and paging.get("links"):
            offset = paging["start"] + paging["count"]
            return {"start": offset, "count": paging["count"]}
        return None

    def request_params(self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> MutableMapping[str, Any]:
        params = super().request_params(stream_state, stream_slice, next_page_token)
        if next_page_token: 
            params.update(next_page_token)
        return params
    
    def parse_response(
        self, response: requests.Response, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None
    ) -> Iterable[Mapping]:
        return [response.json()]

    def should_retry(self, response: requests.Response) -> bool:
        if response.status_code == 429:
            error_message = (
                f"Stream {self.name}: LinkedIn API requests are rate limited. "
                f"Rate limits specify the maximum number of API calls that can be made in a 24 hour period. "
                f"These limits reset at midnight UTC every day. "
                f"You can find more information here https://docs.airbyte.com/integrations/sources/linkedin-pages. "
                f"Also quotas and usage are here: https://www.linkedin.com/developers/apps."
            )
            self.logger.error(error_message)
        return super().should_retry(response)

class LinkedinPagesIncremental(LinkedinPagesStream, IncrementalMixin):
    cursor_field = "start"
    primary_key = "start"

    def __init__(self, config, start_date: datetime, **kwargs):
        super().__init__(config=config)
        self.start_date = start_date
        self._cursor_value = None

    @property
    def state(self) -> Mapping[str, Any]:
        if self._cursor_value:
            return {self.cursor_field: self._cursor_value.strftime('%Y-%m-%d')}
        elif self.start_date:
            return {self.cursor_field: self.start_date.strftime("%Y-%m-%d")}
    
    def request_params(self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> MutableMapping[str, Any]:
        params = super().request_params(stream_state, stream_slice, next_page_token)
        if self.state:
            params.update({
                "timeIntervals.timeGranularityType": "DAY",
                "timeIntervals.timeRange.start": pendulum.from_format(self.state[self.cursor_field], "YYYY-MM-DD").int_timestamp * 1000,
                "timeIntervals.timeRange.end": datetime.now().timestamp() * 1000
            })
        return params

    @state.setter
    def state(self, value: Mapping[str, Any]):
        self._cursor_value = pendulum.from_format(value[self.cursor_field], "YYYY-MM-DD")

    def read_records(self, *args, **kwargs) -> Iterable[Mapping[str, Any]]:
        for record in super().read_records(*args, **kwargs):
            # The time range for LinkedIn Pages is saved in a nested dict
            # This way it is put in the main record and can be parsed easier.
            time_range = record.get("timeRange")
            if time_range:
                record.update(time_range)
                record.pop("timeRange")
            if self._cursor_value:
                latest_record_date = pendulum.from_timestamp(record[self.cursor_field]/1000)
                self._cursor_value = max(self._cursor_value, latest_record_date)
            yield record



class OrganizationLookup(LinkedinPagesStream):

    def path(self, stream_state: Mapping[str, Any], **kwargs) -> MutableMapping[str, Any]:

        path = f"organizations/{self.org}"
        return path


class FollowerStatistics(LinkedinPagesIncremental):
    def path(self, stream_state: Mapping[str, Any], **kwargs) -> MutableMapping[str, Any]:

        path = f"organizationalEntityFollowerStatistics?q=organizationalEntity&organizationalEntity=urn:li:organization:{self.org}"
        return path

    def parse_response(
        self, response: requests.Response, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None
    ) -> Iterable[Mapping]:
        yield from response.json().get("elements")


class ShareStatistics(LinkedinPagesIncremental):
    """https://learn.microsoft.com/en-us/linkedin/marketing/integrations/community-management/organizations/share-statistics
    """

    def path(self, stream_state: Mapping[str, Any], **kwargs) -> MutableMapping[str, Any]:
        path = "organizationalEntityShareStatistics"
        return path

    def request_params(self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> MutableMapping[str, Any]:
        params = super().request_params(stream_state, stream_slice, next_page_token)
        params.update({
            "q": "organizationalEntity",
            "organizationalEntity": f"urn%3Ali%3Aorganization%3A{self.org}"
        })
        return params

    def parse_response(
        self, response: requests.Response, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None
    ) -> Iterable[Mapping]:
        yield from response.json().get("elements")


class TotalFollowerCount(LinkedinPagesStream):
    def path(self, stream_state: Mapping[str, Any], **kwargs) -> MutableMapping[str, Any]:
        path = f"networkSizes/urn:li:organization:{self.org}?edgeType=CompanyFollowedByMember"
        return path


class SourceLinkedinPages(AbstractSource):
    """
    Abstract Source inheritance, provides:
    - implementation for `check` connector's connectivity
    - implementation to call each stream with it's input parameters.
    """

    @classmethod
    def get_authenticator(cls, config: Mapping[str, Any]) -> TokenAuthenticator:
        """
        Validate input parameters and generate a necessary Authentication object
        This connectors support 2 auth methods:
        1) direct access token with TTL = 2 months
        2) refresh token (TTL = 1 year) which can be converted to access tokens
           Every new refresh revokes all previous access tokens q
        """
        auth_method = config.get("credentials", {}).get("auth_method")
        if not auth_method or auth_method == "access_token":
            # support of backward compatibility with old exists configs
            access_token = config["credentials"]["access_token"] if auth_method else config["access_token"]
            return TokenAuthenticator(token=access_token)
        elif auth_method == "oAuth2.0":
            return Oauth2Authenticator(
                token_refresh_endpoint="https://www.linkedin.com/oauth/v2/accessToken",
                client_id=config["credentials"]["client_id"],
                client_secret=config["credentials"]["client_secret"],
                refresh_token=config["credentials"]["refresh_token"],
            )
        raise Exception("incorrect input parameters")

    def check_connection(self, logger: AirbyteLogger, config: Mapping[str, Any]) -> Tuple[bool, any]:
        # RUN $ python main.py check --config secrets/config.json

        """
        Testing connection availability for the connector.
        :: for this check method the Customer must have the "r_liteprofile" scope enabled.
        :: more info: https://docs.microsoft.com/linkedin/consumer/integrations/self-serve/sign-in-with-linkedin
        """

        config["authenticator"] = self.get_authenticator(config)
        stream = OrganizationLookup(config)
        stream.records_limit = 1
        try:
            next(stream.read_records(sync_mode=SyncMode.full_refresh), None)
            return True, None
        except Exception as e:
            return False, e

        # RUN: $ python main.py read --config secrets/config.json --catalog integration_tests/configured_catalog.json

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        config["authenticator"] = self.get_authenticator(config)
        if config.get("start_date"):
            start_date = pendulum.from_format(config['start_date'], 'YYYY-MM-DD')
        else:
            start_date = None
        return [OrganizationLookup(config), FollowerStatistics(config, start_date), ShareStatistics(config, start_date), TotalFollowerCount(config)]
