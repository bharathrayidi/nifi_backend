from fastapi import HTTPException
import requests
import pandas as pd
import traceback
from typing import Dict, Any, List, Optional

DEFAULT_HEADER = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36"
}

REQUEST_HEADER = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9,hi;q=0.8"
}

# Use your existing session‐management function to get an NSE session
def get_large_deals_snapshot(session: requests.Session, timeout: int = 10) -> Dict[str, Any]:
    """
    Fetches the Large Deals snapshot (bulk, block, short) from NSE
    and returns structured data.
    """
    url = "https://www.nseindia.com/api/snapshot-capital-market-largedeal"
    try:
        resp = session.get(url, headers=REQUEST_HEADER, timeout=timeout)
        resp.raise_for_status()
        data = resp.json()
    except requests.exceptions.RequestException as e:
        raise HTTPException(status_code=503, detail=f"Failed to fetch large deals snapshot: {e}")
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Invalid JSON from large deals endpoint: {e}")

    # Extract fields based on actual keys
    as_on_date = data.get("as_on_date")

    bulk_deals = data.get("BULK_DEALS_DATA", [])
    block_deals = data.get("BLOCK_DEALS_DATA", [])
    short_deals = data.get("SHORT_DEALS_DATA", [])

    # Convert into DataFrames for processing
    bulk_df = pd.DataFrame(bulk_deals)
    block_df = pd.DataFrame(block_deals)
    short_df = pd.DataFrame(short_deals)

    # Clean numeric fields if present
    def clean_df(df: pd.DataFrame, numeric_fields: List[str]) -> pd.DataFrame:
        for fld in numeric_fields:
            if fld in df.columns:
                df[fld] = pd.to_numeric(df[fld], errors="coerce").fillna(0)
        return df

    # Adjust field names based on real JSON structure
    numeric_fields = ["QTY_TRADED", "TRADE_PRICE"]  # example, adapt as needed
    bulk_df = clean_df(bulk_df, numeric_fields)
    block_df = clean_df(block_df, numeric_fields)
    short_df = clean_df(short_df, numeric_fields)

    summary = {
        "bulkCount": len(bulk_df),
        "blockCount": len(block_df),
        "shortCount": len(short_df),
    }

    return {
        "asOnDate": as_on_date,
        "bulkDeals": bulk_df.to_dict(orient="records"),
        "blockDeals": block_df.to_dict(orient="records"),
        "shortDeals": short_df.to_dict(orient="records"),
        "summary": summary,
        # Keeping raw sections if you want direct data
        "BULK_DEALS": data.get("BULK_DEALS", []),
        "BLOCK_DEALS": data.get("BLOCK_DEALS", []),
        "SHORT_DEALS": data.get("SHORT_DEALS", []),
    }

def get_volume_gainers(session: requests.Session, timeout: int = 10) -> Dict[str, Any]:
    """
    Fetches the live volume gainers snapshot from NSE and returns structured data.
    """
    url = "https://www.nseindia.com/api/live-analysis-volume-gainers"
    try:
        resp = session.get(url, headers=REQUEST_HEADER, timeout=timeout)
        resp.raise_for_status()
        data = resp.json()
    except requests.exceptions.RequestException as e:
        raise HTTPException(status_code=503, detail=f"Failed to fetch volume gainers: {e}")
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Invalid JSON from volume gainers endpoint: {e}")

    records = data.get("data", [])
    ts = data.get("timestamp")

    df = pd.DataFrame(records)

    # Clean numeric fields
    numeric_fields = [
        "volume", "week1AvgVolume", "week1volChange", "week2AvgVolume",
        "week2volChange", "ltp", "pChange", "turnover"
    ]
    for fld in numeric_fields:
        if fld in df.columns:
            df[fld] = pd.to_numeric(df[fld], errors="coerce").fillna(0)

    # Add ranking by volume change
    if "week1volChange" in df.columns:
        df = df.sort_values(by="week1volChange", ascending=False).reset_index(drop=True)

    summary = {
        "topSymbol": df.iloc[0]["symbol"] if not df.empty else None,
        "totalCount": len(df),
        "timestamp": ts
    }

    return {
        "volumeGainers": df.to_dict(orient="records"),
        "summary": summary
    }

def get_most_active_securities(session: requests.Session, timeout: int = 10) -> Dict[str, Any]:
    """
    Fetches the most active securities snapshot from NSE and returns structured data.
    """
    url = "https://www.nseindia.com/api/live-analysis-most-active-securities?index=value"
    try:
        resp = session.get(url, headers=REQUEST_HEADER, timeout=timeout)
        resp.raise_for_status()
        data = resp.json()
    except requests.exceptions.RequestException as e:
        raise HTTPException(status_code=503, detail=f"Failed to fetch most active securities: {e}")
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Invalid JSON from most active securities endpoint: {e}")

    records = data.get("data", [])
    ts = data.get("timestamp")

    df = pd.DataFrame(records)

    # Clean numeric fields
    numeric_fields = [
        "lastPrice", "pChange", "quantityTraded", "totalTradedVolume",
        "totalTradedValue", "previousClose", "yearHigh", "yearLow",
        "change", "open", "closePrice", "dayHigh", "dayLow"
    ]
    for fld in numeric_fields:
        if fld in df.columns:
            df[fld] = pd.to_numeric(df[fld], errors="coerce").fillna(0)

    # Sort by total traded value (most active by value)
    if "totalTradedValue" in df.columns:
        df = df.sort_values(by="totalTradedValue", ascending=False).reset_index(drop=True)

    summary = {
        "topSymbol": df.iloc[0]["symbol"] if not df.empty else None,
        "topTurnover": df.iloc[0]["totalTradedValue"] if not df.empty else None,
        "totalCount": len(df),
        "timestamp": ts
    }

    return {
        "mostActiveSecurities": df.to_dict(orient="records"),
        "summary": summary
    }

def get_most_active_securities(session: requests.Session, timeout: int = 10) -> Dict[str, Any]:
    """
    Fetches the most active securities snapshot from NSE and returns structured data.
    """
    url = "https://www.nseindia.com/api/live-analysis-most-active-securities?index=value"
    try:
        resp = session.get(url, headers=REQUEST_HEADER, timeout=timeout)
        resp.raise_for_status()
        data = resp.json()
    except requests.exceptions.RequestException as e:
        raise HTTPException(status_code=503, detail=f"Failed to fetch most active securities: {e}")
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Invalid JSON from most active securities endpoint: {e}")

    records = data.get("data", [])
    ts = data.get("timestamp")

    df = pd.DataFrame(records)

    # Clean numeric fields
    numeric_fields = [
        "lastPrice", "pChange", "quantityTraded", "totalTradedVolume",
        "totalTradedValue", "previousClose", "yearHigh", "yearLow",
        "change", "open", "closePrice", "dayHigh", "dayLow"
    ]
    for fld in numeric_fields:
        if fld in df.columns:
            df[fld] = pd.to_numeric(df[fld], errors="coerce").fillna(0)

    # Sort by total traded value (most active by value)
    if "totalTradedValue" in df.columns:
        df = df.sort_values(by="totalTradedValue", ascending=False).reset_index(drop=True)

    summary = {
        "topSymbol": df.iloc[0]["symbol"] if not df.empty else None,
        "topTurnover": df.iloc[0]["totalTradedValue"] if not df.empty else None,
        "totalCount": len(df),
        "timestamp": ts
    }

    return {
        "mostActiveSecurities": df.to_dict(orient="records"),
        "summary": summary
    }
