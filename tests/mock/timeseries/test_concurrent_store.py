import threading

import pandas as pd
import pytest

import cwms.timeseries.timeseries as ts


def test_chunked_store_creates_series_before_parallel_writes(monkeypatch):
    created = threading.Event()
    lock = threading.Lock()
    received = []
    initializing = False
    data = {
        "name": "Test.Stage.Inst.15Minutes.0.Raw",
        "office-id": "MVP",
        "units": "ft",
        "values": [[i, i, 0] for i in range(8)],
    }

    def post(endpoint, chunk, params):
        nonlocal initializing
        with lock:
            first = not initializing
            initializing = True
        if first:
            # Model the database transaction that creates a new series. Other
            # writes cannot use its identifier until that transaction commits.
            assert not created.wait(0.1)
            created.set()
        else:
            assert created.is_set(), "concurrent write raced series creation"
        assert endpoint == "timeseries"
        assert params["store-rule"] == "REPLACE_ALL"
        assert chunk["office-id"] == "MVP"
        with lock:
            received.extend(chunk["values"])

    monkeypatch.setattr(ts.api, "post", post)
    ts.store_timeseries(data, chunk_size=2, store_rule="REPLACE_ALL")
    assert sorted(received) == data["values"]


def test_initial_chunk_failure_stops_remaining_writes(monkeypatch):
    calls = []

    def post(endpoint, chunk, params):
        calls.append(chunk["values"])
        raise ValueError("cannot create series")

    monkeypatch.setattr(ts.api, "post", post)
    with pytest.raises(ValueError, match="cannot create series"):
        ts.store_timeseries(
            {
                "name": "Test.Stage.Inst.15Minutes.0.Raw",
                "office-id": "MVP",
                "units": "ft",
                "values": [[i, i, 0] for i in range(6)],
            },
            chunk_size=2,
        )
    assert calls
    assert all(chunk == [[0, 0, 0], [1, 1, 0]] for chunk in calls)


def test_multi_store_reports_all_failed_series(monkeypatch):
    attempted = []
    lock = threading.Lock()
    data = pd.DataFrame(
        {
            "date-time": pd.to_datetime(["2025-01-01"] * 3, utc=True),
            "value": [1, 2, 3],
            "ts_id": ["good", "bad-one", "bad-two"],
            "units": ["ft"] * 3,
        }
    )

    def store(data, **kwargs):
        with lock:
            attempted.append(data["name"])
        if data["name"].startswith("bad"):
            raise ValueError("write rejected")

    monkeypatch.setattr(ts, "store_timeseries", store)
    with pytest.raises(RuntimeError) as error:
        ts.store_multi_timeseries_df(data, "MVP")
    assert "bad-one" in str(error.value)
    assert "bad-two" in str(error.value)
    assert "write rejected" in str(error.value)
    assert sorted(attempted) == ["bad-one", "bad-two", "good"]
