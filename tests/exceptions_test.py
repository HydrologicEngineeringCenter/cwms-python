#  Copyright (c) 2024
#  United States Army Corps of Engineers - Hydrologic Engineering Center (USACE/HEC)
#  All Rights Reserved.  USACE PROPRIETARY/CONFIDENTIAL.
#  Source may not be released without written approval from HEC


import json
import unittest

from requests.models import PreparedRequest, Response

from cwms.exceptions import ClientError, NoDataFoundError, ServerError
from cwms.utils import raise_for_status
from tests._test_utils import read_resource_file

_ERROR_CODE_500_JSON = read_resource_file("error_code_500.json")


class TestCwmsDataApiError(unittest.TestCase):

    def test_server_error(self):
        with self.assertRaises(ServerError) as e:
            raise_for_status(response=self.mock_response(500))
        self.assertIn(json.dumps(_ERROR_CODE_500_JSON), e.exception.args[0])
        self.assertIsNotNone(e.exception.response)
        self.assertIsNotNone(e.exception.request)
        self.assertIsNotNone(e.exception.incident_identifier)

    def test_client_error(self):
        with self.assertRaises(ClientError) as e:
            raise_for_status(response=self.mock_response(400))
        self.assertIn(json.dumps(_ERROR_CODE_500_JSON), e.exception.args[0])
        self.assertIsNotNone(e.exception.response)
        self.assertIsNotNone(e.exception.request)
        self.assertIsNotNone(e.exception.incident_identifier)

    def test_no_data_found_error(self):
        with self.assertRaises(NoDataFoundError) as e:
            raise_for_status(response=self.mock_response(404))
        self.assertIn(json.dumps(_ERROR_CODE_500_JSON), e.exception.args[0])
        self.assertIsNotNone(e.exception.response)
        self.assertIsNotNone(e.exception.request)
        self.assertIsNotNone(e.exception.incident_identifier)

    def mock_response(self, status_code: int):
        response = Response()
        response.status_code = status_code
        json_string = json.dumps(_ERROR_CODE_500_JSON)
        response._content = json_string.encode("utf-8")
        response.request = PreparedRequest()
        response.request.url = (
            "https://mockwebserver.cwms.gov/cwms-data/hello-world?parameter=true"
        )
        return response


if __name__ == "__main__":
    unittest.main()
