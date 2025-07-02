import pytest
import cwms
from unittest.mock import patch

def pytest_addoption(parser):
    parser.addoption(
        "--api_key",
        action="store",
        default='0123456789abcdef0123456789abcdef',
        help="Set a custom API key for the CWMS API"
    )
    parser.addoption(
        "--api_root",
        action="store",
        default='http://localhost:8082/cwms-data/',
        help="Set a custom API root for the CWMS API"
    )

@pytest.fixture(scope="package", autouse=True)
def auto_track_locations(request):
    api_key = request.config.getoption("api_key")
    api_root = request.config.getoption("api_root")
    cwms.api.init_session(api_root=api_root, api_key=api_key)

    print(f"Test api_root and api_key: {api_root}, {api_key}")

    created_locations = set()

    original_store_location = cwms.store_location

    def store_location_wrapper(location_dict):
        result = original_store_location(location_dict)
        location_id = location_dict.get("name")
        office_id = location_dict.get("office-id")
        if location_id and office_id:
            if "-" in location_id:
                base_location_id = location_id.split("-")[0]
                created_locations.add((base_location_id, office_id))
            else:
                created_locations.add((location_id, office_id))
        return result

    patcher = patch.object(cwms, "store_location", store_location_wrapper)
    patcher.start()

    def cleanup():
        print("Cleaning up created locations...")
        print(len(created_locations), "Base locations created during the test session.")
        patcher.stop()
        for location_id, office_id in created_locations:
            try:
                cwms.delete_location(location_id, office_id)
            except Exception as e:
                print(f"Failed to delete location {location_id}: {e}")
        cwms.api.init_session(api_root=cwms.api.API_ROOT)

    request.addfinalizer(cleanup)