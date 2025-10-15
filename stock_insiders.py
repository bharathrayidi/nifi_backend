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


def get_corporates_pit(session: requests.Session, timeout: int = 10) -> Dict[str, Any]:
    """
    Fetches insider trading disclosures (PIT data) from NSE corporates API.
    Handles both 'acqNameList' (master acquirer names) and 'data' (transactions).
    """
    url = "https://www.nseindia.com/api/corporates-pit"
    try:
        resp = session.get(url, headers=REQUEST_HEADER, timeout=timeout)
        # resp.raise_for_status()
        data = resp.json()
    except requests.exceptions.RequestException as e:
        raise HTTPException(status_code=503, detail=f"Failed to fetch corporates PIT data: {e}")
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Invalid JSON from corporates PIT endpoint: {e}")

    acq_name_list = data.get("acqNameList", [])
    records = data.get("data", [])

    # Convert to DataFrame
    df = pd.DataFrame(records)

    if df.empty:
        return {
            "corporatesPIT": [],
            "acqNameList": acq_name_list,
            "summary": {"totalDisclosures": 0}
        }

    # Clean numeric fields
    numeric_fields = [
        "buyValue", "sellValue", "buyQuantity", "sellquantity",
        "secAcq", "secVal", "afterAcqSharesNo", "afterAcqSharesPer",
        "befAcqSharesNo", "befAcqSharesPer"
    ]
    for fld in numeric_fields:
        if fld in df.columns:
            df[fld] = pd.to_numeric(df[fld], errors="coerce").fillna(0)

    # Parse date column if available
    if "date" in df.columns:
        try:
            df["date"] = pd.to_datetime(df["date"], errors="coerce", format="%d-%b-%Y %H:%M")
            df = df.sort_values(by="date", ascending=False).reset_index(drop=True)
        except Exception:
            pass

    # Aggregate analysis
    buy_total = df[df["tdpTransactionType"].str.upper() == "BUY"]["secVal"].sum() if "tdpTransactionType" in df.columns else 0
    sell_total = df[df["tdpTransactionType"].str.upper() == "SELL"]["secVal"].sum() if "tdpTransactionType" in df.columns else 0

    # Company-wise grouping
    company_summary = {}
    if "company" in df.columns:
        company_group = df.groupby("company").agg({
            "secVal": "sum",
            "tdpTransactionType": "count"
        }).reset_index()
        company_summary = company_group.to_dict(orient="records")

    summary = {
        "latestDisclosure": df.iloc[0].to_dict(),
        "totalDisclosures": len(df),
        "buyValueTotal": float(buy_total),
        "sellValueTotal": float(sell_total),
        "companySummary": company_summary
    }

    return {
        "corporatesPIT": df.to_dict(orient="records"),
        "acqNameList": acq_name_list,
        "summary": summary
    }
