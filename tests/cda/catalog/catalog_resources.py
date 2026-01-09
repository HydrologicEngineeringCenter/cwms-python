from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Optional

import pandas as pd


@dataclass(frozen=True)
class CatalogResource:
    name: str
    id_key: str                 # "blob_id" or "clob_id" for delete/get args
    id_col_fallback: str        # "blob-id" or "clob-id" in listing df
    list_fn: Callable[..., Any] # get_blobs/get_clobs
    store_fn: Callable[..., Any]
    get_fn: Callable[..., Any]  # get_blob/get_clob
    update_fn: Callable[..., Any]
    delete_fn: Callable[..., Any]
    extract_content: Callable[[Any], str]  # how to get string content from get()


def df_from_result(res: Any) -> Optional[pd.DataFrame]:
    if isinstance(res, pd.DataFrame):
        return res
    return getattr(res, "df", None)


def find_row(resource: CatalogResource, office: str, item_id: str) -> Optional[pd.Series]:
    res = resource.list_fn(office_id=office, **{f"{resource.id_key}_like": item_id})
    df = df_from_result(res)
    if df is None or df.empty:
        return None
    if "id" not in df.columns and resource.id_col_fallback in df.columns:
        df = df.rename(columns={resource.id_col_fallback: "id"})
    match = df[df["id"].astype(str).str.upper() == item_id.upper()]
    return match.iloc[0] if not match.empty else None
