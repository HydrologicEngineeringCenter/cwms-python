import pytest
from pandas import DataFrame

from cwms.types import Data


@pytest.fixture
def test_object():
    return {
        "foo": {
            "bar": [
                {
                    "col1": 1,
                    "col2": 2,
                    "col3": 3,
                }
            ],
        },
        "baz": [
            {
                "col1": 4,
                "col2": 5,
                "col3": 6,
            }
        ],
    }


@pytest.fixture
def test_list(test_object):
    return [test_object["foo"]["bar"][0], test_object["baz"][0]]


def test_to_df(test_object, test_list):
    """Data can be extracted into a data frame."""

    # Create a data frame with a nested selector.
    df = Data.to_df(test_object, "foo.bar")

    # Verify that a data frame is returned with the correct values.
    assert type(df) == DataFrame
    assert df.to_numpy().tolist() == [[1, 2, 3]]

    # Create a data frame with a single, top-level selector.
    df = Data.to_df(test_object, "baz")

    assert type(df) == DataFrame
    assert df.to_numpy().tolist() == [[4, 5, 6]]

    # Create a data frame without a selector.
    df = Data.to_df(test_list, None)

    assert type(df) == DataFrame
    assert df.to_numpy().tolist() == [[1, 2, 3], [4, 5, 6]]


def test_data_init(test_object, test_list):
    """Data wrapper is initialized correctly."""

    # Create a data wrapper with a nested selector.
    data = Data(test_object, selector="foo.bar")

    # The data should be stored on the object and the selector should be set.
    assert data.json == test_object
    assert data.selector == "foo.bar"

    # Create a data wrapper without a selector.
    data = Data(test_list)

    # In this case, the data selector should be unassigned.
    assert data.json == test_list
    assert data.selector == None


def test_df_property(test_object):
    """Verify that the data frame is generated and cached."""

    # Create a data wrapper with a nested selector.
    data = Data(test_object, selector="foo.bar")

    # The data frame cache should initially be None.
    assert data._df == None

    # Access the df property. This should cause the data frame object to be generated and
    # cached.
    df = data.df
    assert type(df) == DataFrame
    assert df.to_numpy().tolist() == [[1, 2, 3]]

    # Verify that the data has been cached.
    assert type(data._df) == DataFrame
    assert data._df.compare(df).empty

    # Finally, confirm that the original JSON data has not been modified.
    assert data.json == test_object
