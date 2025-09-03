from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

import cwms
import cwms.ratings.ratings as ratings
import cwms.ratings.ratings_spec as ratings_spec

TEST_OFFICE = "MVP"
TEST_RATING_ID = "pytest_rating.Stage;ft.Flow;cfs.Linear.Production"


@pytest.fixture(scope="module", autouse=True)
def setup_data():
    # Create rating spec for testing
    spec_json = {
        "office-id": TEST_OFFICE,
        "rating-spec-id": TEST_RATING_ID,
        "template-id": "Linear",
        "ind-parameter": "Stage;ft",
        "dep-parameter": "Flow;cfs",
    }
    ratings_spec.store_rating_spec(spec_json)

    yield

    # Clean up
    ratings_spec.delete_rating_spec(TEST_RATING_ID, TEST_OFFICE)


@pytest.fixture(autouse=True)
def init_session():
    print("Initializing CWMS API session for ratings tests...")


def test_rating_simple_df_to_json():
    now = datetime.now(timezone.utc).replace(microsecond=0)
    df = pd.DataFrame({"ind": [1.0, 2.0], "dep": [10.0, 20.0]})

    json_out = ratings.rating_simple_df_to_json(
        data=df,
        rating_id=TEST_RATING_ID,
        office_id=TEST_OFFICE,
        units="ft;cfs",
        effective_date=now,
        description="pytest rating curve",
    )

    assert "simple-rating" in json_out
    assert json_out["simple-rating"]["rating-spec-id"] == TEST_RATING_ID
    assert json_out["simple-rating"]["units-id"] == "ft;cfs"
    assert len(json_out["simple-rating"]["rating-points"]["point"]) == 2


def test_store_rating_and_get_current_rating():
    now = datetime.now(timezone.utc).replace(microsecond=0)
    df = pd.DataFrame({"ind": [1.0, 2.0], "dep": [10.0, 20.0]})

    rating_json = ratings.rating_simple_df_to_json(
        df, TEST_RATING_ID, TEST_OFFICE, "ft;cfs", effective_date=now
    )
    ratings.store_rating(rating_json)

    data = ratings.get_current_rating(TEST_RATING_ID, TEST_OFFICE)
    assert data.df is not None
    assert not data.df.empty
    assert pytest.approx(data.df["dep"].iloc[0]) == 10.0


def test_get_current_rating_xml():
    xml_data = ratings.get_current_rating_xml(TEST_RATING_ID, TEST_OFFICE)
    assert isinstance(xml_data, str)
    assert xml_data.startswith("<?xml")


def test_rate_values_and_reverse():
    result = ratings.rate_values(
        rating_id=TEST_RATING_ID,
        office_id=TEST_OFFICE,
        units="ft;cfs",
        values=[[1.0, 2.0]],
    )
    assert "values" in result

    # Reverse rating
    dep_values = [r[0] for r in result["values"]]  # dependent outputs
    rev_result = ratings.reverse_rate_values(
        rating_id=TEST_RATING_ID,
        office_id=TEST_OFFICE,
        units="ft;cfs",
        values=dep_values,
    )
    assert "values" in rev_result


def test_delete_ratings():
    now = datetime.now(timezone.utc).replace(microsecond=0)
    begin = now - timedelta(hours=1)
    end = now + timedelta(hours=1)

    # Should not hit
    ratings.delete_ratings(TEST_RATING_ID, TEST_OFFICE, begin, end)
