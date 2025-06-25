import pandas as pd
import pytest


import cwms
import cwms.api


@pytest.fixture(autouse=True)
def init_session():
    cwms.api.init_session(api_root='http://localhost:8081/cwms-data/', api_key='1234567890abcdef1234567890abcdef')

def test_get_location_operations():
    """
    Test the retrieval of location operations from the CWMS API.
    """
    loc_cat = cwms.get_locations_catalog(office_id='SPK')
    
    assert(len(loc_cat.df)==0)  # Assuming no locations are present for SPK office in the test environment