from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import requests
import pandas as pd
from typing import Optional, List, Dict, Any
import warnings
import time
import traceback
from datetime import datetime, timedelta
from news import YahooNewsFetcher
from bluck_deal import get_large_deals_snapshot, get_volume_gainers, get_most_active_securities
from stock_insiders import get_corporates_pit
warnings.filterwarnings('ignore', category=FutureWarning)
pd.options.mode.copy_on_write = True

app = FastAPI(title="Nifty Options Backend")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials=True,
)

ORIGIN_URL = "https://www.nseindia.com/market-data/most-active-contracts"
URL_ACTIVE = "https://www.nseindia.com/api/NextApi/apiClient/indexTrackerApi?functionName=getMostActiveContracts&&index=NIFTY"
URL_ADVANCE_DECLINE = "https://www.nseindia.com/api/NextApi/apiClient/indexTrackerApi?functionName=getAdvanceDecline&&index=NIFTY%2050"
URL_INDEX_DATA = "https://www.nseindia.com/api/NextApi/apiClient/indexTrackerApi?functionName=getIndexData&&index=NIFTY%2050"

# Define a base WITHOUT the expiry parameter. We'll add it dynamically.
OPTION_CHAIN_BASE_URL_WITHOUT_EXPIRY_PARAM = "https://www.nseindia.com/api/option-chain-v3?type=Indices&symbol=NIFTY"

DEFAULT_HEADER = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36"
}

REQUEST_HEADER = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9,hi;q=0.8"
}

_session: Optional[requests.Session] = None
_last_session_update: float = 0
SESSION_REFRESH_INTERVAL = 300

def get_nse_session(timeout: int = 10) -> requests.Session:
    global _session, _last_session_update
    
    if _session is None or (time.time() - _last_session_update) > SESSION_REFRESH_INTERVAL:
        print("Attempting to refresh NSE session and cookies...")
        s = requests.Session()
        s.headers.update(DEFAULT_HEADER)
        
        try:
            # First, fetch cookies from the main URL
            s.get(ORIGIN_URL, headers=DEFAULT_HEADER, timeout=timeout)
            # Then, ensure the session can access an API endpoint as well
            # This helps to get necessary API-specific cookies if any, and maintain session.
            s.get(URL_INDEX_DATA, headers=REQUEST_HEADER, timeout=timeout) 
            _session = s
            _last_session_update = time.time()
            print("NSE session refreshed successfully.")
        except requests.exceptions.RequestException as e:
            print(f"Failed to refresh NSE session: {e}")
            if _session is None:
                raise HTTPException(status_code=503, detail="Could not establish initial connection to NSE. Please try again later.")
            print("Using existing (potentially stale) session due to refresh failure.")
    return _session

def get_nearest_expiry_from_nse(session: requests.Session) -> Optional[str]:
    """
    Fetches the list of expiry dates from NSE and returns the nearest one in 'DD-Mon-YYYY' format.
    """
    try:
        expiry_fetch_url = OPTION_CHAIN_BASE_URL_WITHOUT_EXPIRY_PARAM
        
        print(f"Fetching expiry dates from: {expiry_fetch_url}")
        res = session.get(expiry_fetch_url, headers=REQUEST_HEADER, timeout=10)
        res.raise_for_status() # Raise an exception for HTTP errors
        
        data = res.json()
        expiry_dates = data.get('records', {}).get('expiryDates', [])
        
        if expiry_dates:
            # NSE returns dates in 'DD-Mon-YYYY' format, which is what we need for internal use.
            return expiry_dates[0] 
        else:
            print("No expiry dates found in NSE response.")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Error fetching expiry dates from NSE: {e}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred while processing expiry dates: {e}")
        traceback.print_exc()
        return None

def get_next_weekday(start_date: datetime, weekday: int) -> datetime:
    """
    Calculates the next occurrence of a specific weekday.
    Monday is 0, Tuesday is 1, ..., Sunday is 6.
    """
    days_ahead = weekday - start_date.weekday()
    if days_ahead <= 0:
        days_ahead += 7
    return start_date + timedelta(days=days_ahead)

def format_date_for_nse_internal(dt: datetime) -> str:
    """
    Formats a datetime object into 'DD-Mon-YYYY' e.g., '02-Sep-2025' for internal use and display.
    This is the format NSE uses in its API responses for expiry dates.
    """
    return dt.strftime('%d-%b-%Y') 

def format_date_for_nse_url(dt: datetime) -> str:
    """
    Formats a datetime object into 'DD-MM-YYYY' e.g., '02-09-2025' for the NSE API URL parameter.
    """
    # NSE URL parameter expects DD-MM-YYYY, but month as short name e.g., 02-Sep-2025 not 02-09-2025
    # The original implementation of NSE uses month abbreviations, so let's stick to that if it works.
    # If the API truly expects '02-09-2025', this function needs to change.
    # Based on observation, NSE APIs often use 'DD-Mon-YYYY' even in URL params.
    return dt.strftime('%d-%b-%Y')


def generate_alerts(valuation_df: pd.DataFrame, options_df: pd.DataFrame) -> Dict[str, Any]:
    print(f"Generating alerts based on valuation and options data...")
    # print(f"Valuation DF Head:\n{valuation_df.head()}")
    # print(f"Options DF Head:\n{options_df.head()}")

    alerts_data = {
        "fairValuation": "Valuation data unavailable.",
        "marketBreadth": "Market breadth data unavailable.",
        "buyingInterest": "Buying interest data unavailable.",
        "momentum": [],
        "unwinding": [],
        "freshLongs": [],
        "buyerDominance": [],
        "sellerDominance": [],
    }

    if not valuation_df.empty:
        # Ensure numeric conversion for all stats
        pe = pd.to_numeric(valuation_df.get('peRatio', pd.Series([0.0])), errors='coerce').iloc[0] if not valuation_df.empty else 0.0
        adv = pd.to_numeric(valuation_df.get('advance_symbol', pd.Series([0])), errors='coerce').iloc[0] if not valuation_df.empty else 0
        dec = pd.to_numeric(valuation_df.get('decline_symbol', pd.Series([0])), errors='coerce').iloc[0] if not valuation_df.empty else 0
        adv_turn = pd.to_numeric(valuation_df.get('advance_top_turnover', pd.Series([0.0])), errors='coerce').iloc[0] if not valuation_df.empty else 0.0
        dec_turn = pd.to_numeric(valuation_df.get('decline_top_turnover', pd.Series([0.0])), errors='coerce').iloc[0] if not valuation_df.empty else 0.0

        if pd.isna(pe): pe = 0.0
        if pd.isna(adv): adv = 0
        if pd.isna(dec): dec = 0
        if pd.isna(adv_turn): adv_turn = 0.0
        if pd.isna(dec_turn): dec_turn = 0.0

        if pe > 25:
            alerts_data["fairValuation"] = "PE ratio is high, suggesting overvaluation."
        elif pe > 0 and pe < 15:
            alerts_data["fairValuation"] = "PE ratio is low, suggesting undervaluation."
        elif pe > 0:
            alerts_data["fairValuation"] = "PE ratio is within normal range."
        else:
            alerts_data["fairValuation"] = "PE ratio data not available or invalid."

        if adv + dec > 0:
            if adv / (adv + dec) > 0.75:
                alerts_data["marketBreadth"] = "Majority of stocks are advancing (Bullish sentiment)."
            elif dec / (adv + dec) > 0.75:
                alerts_data["marketBreadth"] = "Majority of stocks are declining (Bearish sentiment)."
            else:
                alerts_data["marketBreadth"] = "Mixed sentiment across market breadth."
        else:
            alerts_data["marketBreadth"] = "Market breadth data not available."

        if adv_turn + dec_turn > 0:
            if adv_turn / (adv_turn + dec_turn) > 0.75:
                alerts_data["buyingInterest"] = "Turnover concentrated in advancing stocks (Strong Buying Interest)."
            elif dec_turn / (adv_turn + dec_turn) > 0.75:
                alerts_data["buyingInterest"] = "Turnover concentrated in declining stocks (Strong Selling Interest)."
            else:
                alerts_data["buyingInterest"] = "Turnover is balanced across advancing and declining stocks."
        else:
            alerts_data["buyingInterest"] = "Turnover data not available."
    
    if not options_df.empty:
        seen_momentum = set()
        seen_unwinding = set()
        seen_fresh_longs = set()
        seen_buyer_dominance = set()
        seen_seller_dominance = set()

        # Ensure all columns used for alerts are numeric with robust error handling
        options_for_alerts_df = options_df.copy()
        options_for_alerts_df['pchange'] = pd.to_numeric(options_for_alerts_df['pchange'], errors='coerce').fillna(0)
        options_for_alerts_df['OI'] = pd.to_numeric(options_for_alerts_df['OI'], errors='coerce').fillna(0)
        options_for_alerts_df['COI'] = pd.to_numeric(options_for_alerts_df['COI'], errors='coerce').fillna(0)
        options_for_alerts_df['TBQ'] = pd.to_numeric(options_for_alerts_df['TBQ'], errors='coerce').fillna(0)
        options_for_alerts_df['TSQ'] = pd.to_numeric(options_for_alerts_df['TSQ'], errors='coerce').fillna(0)

        for _, opt in options_for_alerts_df.iterrows():
            strike = opt.get('strikePrice')
            typ = opt.get('optionType')
            
            if strike is None or typ is None:
                continue

            pchange_val = opt['pchange'] # Already numeric
            oi = opt['OI']       # Already numeric
            coi = opt['COI']     # Already numeric
            tbq = opt['TBQ']     # Already numeric
            tsq = opt['TSQ']     # Already numeric
            
            # print(f"Processing option for alerts: Strike={strike}, Type={typ}, PChange={pchange_val}, OI={oi}, COI={coi}, TBQ={tbq}, TSQ={tsq}")
            key = f"{strike} {typ}"
            
            # ⚡ High Momentum: Large percentage change in price
            if abs(pchange_val) > 20: # Adjusted threshold for more alerts, original was 50
                if key not in seen_momentum:
                    alerts_data["momentum"].append(key)
                    seen_momentum.add(key)

            # 🔻 Unwinding Alert: High existing OI, but significant decrease in COI
            if oi > 50000 and coi < -10000: # Adjusted thresholds
                if key not in seen_unwinding:
                    alerts_data["unwinding"].append(key)
                    seen_unwinding.add(key)
            
            # 🟢 Fresh Longs: High existing OI, and significant increase in COI
            elif oi > 50000 and coi > 10000: # Adjusted thresholds
                if key not in seen_fresh_longs:
                    alerts_data["freshLongs"].append(key)
                    seen_fresh_longs.add(key)

            # 🛒 Buyer Dominance: Total Buy Quantity significantly higher than Total Sell Quantity
            if tsq > 0 and tbq > tsq * 1.5 and tbq > 50000: # Added minimum TBQ
                if key not in seen_buyer_dominance:
                    alerts_data["buyerDominance"].append(key)
                    seen_buyer_dominance.add(key)
            
            # 📉 Seller Dominance: Total Sell Quantity significantly higher than Total Buy Quantity
            elif tbq > 0 and tsq > tbq * 1.5 and tsq > 50000: # Added minimum TSQ
                if key not in seen_seller_dominance:
                    alerts_data["sellerDominance"].append(key)
                    seen_seller_dominance.add(key)
        
        # Sort the lists for consistent output
        alerts_data["momentum"] = sorted(list(alerts_data["momentum"]))
        alerts_data["unwinding"] = sorted(list(alerts_data["unwinding"]))
        alerts_data["freshLongs"] = sorted(list(alerts_data["freshLongs"]))
        alerts_data["buyerDominance"] = sorted(list(alerts_data["buyerDominance"]))
        alerts_data["sellerDominance"] = sorted(list(alerts_data["sellerDominance"]))

    return alerts_data

@app.get('/nifty/data', response_model=Dict[str, Any], summary="Get Live Nifty Options Data and Alerts")
async def get_nifty_data():
    print("Attempting to fetch live data from NSE...")
    s = get_nse_session()
    
    try:
        idx_res = s.get(URL_INDEX_DATA, headers=REQUEST_HEADER, timeout=10)
        adv_res = s.get(URL_ADVANCE_DECLINE, headers=REQUEST_HEADER, timeout=10)
        
        idx_json = idx_res.json() if idx_res.ok else {}
        adv_json = adv_res.json() if adv_res.ok else {}
        
        idx_data_df = pd.DataFrame(idx_json.get('data', [{}]))
        adv_data_df = pd.DataFrame(adv_json.get('data', [{}]))
        
        if not idx_data_df.empty and not adv_data_df.empty and 'indexName' in idx_data_df.columns and 'indexName' in adv_data_df.columns:
            stats_df = pd.merge(idx_data_df, adv_data_df, on='indexName', how='inner').fillna(0)
        elif not idx_data_df.empty or not adv_data_df.empty:
            stats_df = pd.concat([idx_data_df, adv_data_df], axis=1).fillna(0)
            stats_df = stats_df.loc[:,~stats_df.columns.duplicated()].copy()
        else:
            stats_df = pd.DataFrame([{}])

        nifty_stats = stats_df.to_dict(orient='records')[0] if not stats_df.empty else {}
        
        default_stats = {'indexName': 'NIFTY 50', 'ffm': 0.0, 'peRatio': 0.0, 'pbRatio': 0.0, 'dividentYield': 0.0, 'volume': 0.0,
                         'advance_symbol': 0, 'decline_symbol': 0, 'unchanged_symbol': 0, 'total_symbol': 50,
                         'advance_top_turnover': 0.0, 'decline_top_turnover': 0.0, 'total_top_turnover': 0.0}
        nifty_stats = {**default_stats, **nifty_stats}


        active_res = s.get(URL_ACTIVE, headers=REQUEST_HEADER, timeout=10)
        active_json = active_res.json() if active_res.ok else {}
        
        mostActiveCall_raw = active_json.get('mostActiveCall', [])
        mostActivePut_raw = active_json.get('mostActivePut', [])
        mostActiveOI_raw = active_json.get('mostActiveContractbyOI', [])

        # --- Expiry Date Generation Logic ---
        nearest_expiry_display_format = get_nearest_expiry_from_nse(s)
        
        if nearest_expiry_display_format is None:
            # Fallback to next Tuesday if NSE doesn't provide expiry dates.
            # Using Tuesday as in your original fallback logic.
            # Use the 'format_date_for_nse_internal' for the display format
            Tuesday = 1 
            next_tuesday_dt = get_next_weekday(datetime.now(), Tuesday)
            nearest_expiry_display_format = format_date_for_nse_internal(next_tuesday_dt)
            print(f"NSE expiry dates not available, falling back to calculated nearest Tuesday: {nearest_expiry_display_format}")
        else:
            print(f"Nearest expiry date from NSE: {nearest_expiry_display_format}")


        # Construct the option chain URL with the determined (or fallback) expiry
        # Convert the 'DD-Mon-YYYY' format to 'DD-MM-YYYY' for the URL parameter
        formatted_expiry_for_url = None
        if nearest_expiry_display_format:
            try:
                dt_obj = datetime.strptime(nearest_expiry_display_format, '%d-%b-%Y')
                formatted_expiry_for_url = format_date_for_nse_url(dt_obj)
            except ValueError:
                print(f"Warning: Could not parse nearest_expiry_display_format '{nearest_expiry_display_format}'.")
                
        if formatted_expiry_for_url:
            option_chain_url = f"{OPTION_CHAIN_BASE_URL_WITHOUT_EXPIRY_PARAM}&expiry={formatted_expiry_for_url}"
        else:
            option_chain_url = OPTION_CHAIN_BASE_URL_WITHOUT_EXPIRY_PARAM # Fallback to no expiry param if none found/calculated
            print("Warning: Could not determine an expiry date, fetching option chain without specific expiry.")

        print(f"Fetching option chain data from: {option_chain_url}")
        oc_res = s.get(option_chain_url, headers=REQUEST_HEADER, timeout=10)
        oc_json = oc_res.json() if oc_res.ok else {}

        option_rows_from_oc = oc_json.get('filtered', {}).get('data', [])
        
        # --- Collect all strike data for optionChainDetails and for alerts ---
        all_option_chain_details = []
        all_options_for_alerts = [] # This will now be populated directly from option_rows_from_oc
        
        for r in option_rows_from_oc:
            strike = r.get('strikePrice')
            if strike is None:
                continue

            ce_data = r.get('CE') if isinstance(r.get('CE'), dict) else {}
            pe_data = r.get('PE') if isinstance(r.get('PE'), dict) else {}
            
            # Use the expiry date from the CE/PE data if available, otherwise fallback
            current_expiry_for_option = ce_data.get('expiryDate') or pe_data.get('expiryDate') or nearest_expiry_display_format

            # Combine CE and PE data for a single strike into one dictionary
            strike_info = {
                'strikePrice': strike,
                'expiryDate': current_expiry_for_option, 
                'CE_openInterest': ce_data.get('openInterest', 0),
                'CE_changeinOpenInterest': ce_data.get('changeinOpenInterest', 0),
                'CE_totalTradedVolume': ce_data.get('totalTradedVolume', 0),
                'CE_impliedVolatility': ce_data.get('impliedVolatility', 0.0),
                'CE_lastPrice': ce_data.get('lastPrice', 0.0),
                'CE_pChange': ce_data.get('pChange', 0.0),
                'CE_totalBuyQuantity': ce_data.get('totalBuyQuantity', 0),
                'CE_totalSellQuantity': ce_data.get('totalSellQuantity', 0),

                'PE_openInterest': pe_data.get('openInterest', 0),
                'PE_changeinOpenInterest': pe_data.get('changeinOpenInterest', 0),
                'PE_totalTradedVolume': pe_data.get('totalTradedVolume', 0),
                'PE_impliedVolatility': pe_data.get('impliedVolatility', 0.0),
                'PE_lastPrice': pe_data.get('lastPrice', 0.0),
                'PE_pChange': pe_data.get('pChange', 0.0),
                'PE_totalBuyQuantity': pe_data.get('totalBuyQuantity', 0),
                'PE_totalSellQuantity': pe_data.get('totalSellQuantity', 0),
            }
            all_option_chain_details.append(strike_info)

            # Also prepare data for alerts directly from here
            # Calls
            if ce_data:
                alert_ce_opt = {
                    'strikePrice': strike,
                    'optionType': 'CE',
                    'expiryDate': current_expiry_for_option,
                    'OI': pd.to_numeric(ce_data.get('openInterest', 0), errors='coerce') or 0,
                    'COI': pd.to_numeric(ce_data.get('changeinOpenInterest', 0), errors='coerce') or 0,
                    'TBQ': pd.to_numeric(ce_data.get('totalBuyQuantity', 0), errors='coerce') or 0,
                    'TSQ': pd.to_numeric(ce_data.get('totalSellQuantity', 0), errors='coerce') or 0,
                    'pchange': pd.to_numeric(ce_data.get('pChange', 0), errors='coerce') or 0,
                    'lastPrice': pd.to_numeric(ce_data.get('lastPrice', 0.0), errors='coerce') or 0.0,
                }
                all_options_for_alerts.append(alert_ce_opt)
            
            # Puts
            if pe_data:
                alert_pe_opt = {
                    'strikePrice': strike,
                    'optionType': 'PE',
                    'expiryDate': current_expiry_for_option,
                    'OI': pd.to_numeric(pe_data.get('openInterest', 0), errors='coerce') or 0,
                    'COI': pd.to_numeric(pe_data.get('changeinOpenInterest', 0), errors='coerce') or 0,
                    'TBQ': pd.to_numeric(pe_data.get('totalBuyQuantity', 0), errors='coerce') or 0,
                    'TSQ': pd.to_numeric(pe_data.get('totalSellQuantity', 0), errors='coerce') or 0,
                    'pchange': pd.to_numeric(pe_data.get('pChange', 0), errors='coerce') or 0,
                    'lastPrice': pd.to_numeric(pe_data.get('lastPrice', 0.0), errors='coerce') or 0.0,
                }
                all_options_for_alerts.append(alert_pe_opt)

        # Create a DataFrame for alerts
        all_options_df_for_alerts = pd.DataFrame(all_options_for_alerts)

        # --- END NEW EXTRACTION for optionChainDetails and all_options_for_alerts ---

        # Create a mapping for quick lookup for `enrich_contracts`
        oc_details_map = {}
        for opt in all_options_for_alerts: # Using the consolidated alert data for the map
            sp = opt.get('strikePrice')
            opt_type_char = opt.get('optionType') # 'CE' or 'PE'
            exp_date = opt.get('expiryDate')

            if sp is not None and opt_type_char in ['CE', 'PE'] and exp_date:
                key = (sp, exp_date)
                if key not in oc_details_map:
                    oc_details_map[key] = {}
                # Store the full dictionary, as enrich_contracts expects detail.get('openInterest') etc.
                oc_details_map[key][opt_type_char] = {
                    'openInterest': opt['OI'],
                    'changeinOpenInterest': opt['COI'],
                    'totalBuyQuantity': opt['TBQ'],
                    'totalSellQuantity': opt['TSQ'],
                    'pChange': opt['pchange'],
                    'lastPrice': opt['lastPrice'],
                    # Add other fields that might be useful for enrichment
                    'totalTradedVolume': opt.get('volume', 0) # Assuming 'volume' might be present in alert_opt if derived
                }
        
        def enrich_contracts(contract_list: List[Dict]) -> List[Dict]:
            enriched = []
            for contract in contract_list:
                sp = contract.get('strikePrice')
                opt_type_raw = contract.get('optionType')
                contract_expiry = contract.get('expiryDate') # This should be in DD-Mon-YYYY
                
                # Normalize option type for lookup
                oc_type_key = 'CE' if opt_type_raw == 'Call' else ('PE' if opt_type_raw == 'Put' else None)

                merged_contract = dict(contract)
                
                # Use the contract's expiry date for lookup, falling back to nearest_expiry_display_format if not present
                lookup_expiry = contract_expiry if contract_expiry else nearest_expiry_display_format
                
                if sp is not None and oc_type_key in ['CE', 'PE'] and lookup_expiry:
                    lookup_key = (sp, lookup_expiry)
                    if lookup_key in oc_details_map:
                        detail = oc_details_map[lookup_key].get(oc_type_key)
                        if isinstance(detail, dict):
                            merged_contract.update({
                                'OI': pd.to_numeric(detail.get('openInterest', 0), errors='coerce') or 0,
                                'COI': pd.to_numeric(detail.get('changeinOpenInterest', 0), errors='coerce') or 0,
                                'TBQ': pd.to_numeric(detail.get('totalBuyQuantity', 0), errors='coerce') or 0,
                                'TSQ': pd.to_numeric(detail.get('totalSellQuantity', 0), errors='coerce') or 0,
                                'pchange': pd.to_numeric(detail.get('pChange', 0), errors='coerce') or 0,
                                'lastPrice': pd.to_numeric(detail.get('lastPrice', 0.0), errors='coerce') or 0.0,
                                'volume': pd.to_numeric(detail.get('totalTradedVolume', 0), errors='coerce') or 0
                            })
                
                merged_contract['optionType'] = opt_type_raw.replace('Call', 'CE').replace('Put', 'PE').replace('Index', 'XX')
                merged_contract['lastPrice'] = pd.to_numeric(merged_contract.get('lastPrice', 0.05), errors='coerce') or 0.05
                merged_contract['volume'] = pd.to_numeric(merged_contract.get('volume', 0), errors='coerce') or 0
                merged_contract['value'] = merged_contract.get('value', '-') # Keep as is, not numeric
                merged_contract['pchange'] = pd.to_numeric(merged_contract.get('pchange', 0), errors='coerce') or 0
                
                # Format OI, COI, Volume for display, but ensure underlying numeric for enrichment
                display_oi = merged_contract.get('OI', 0)
                display_coi = merged_contract.get('COI', 0)
                display_volume = merged_contract.get('volume', 0)

                merged_contract['OI_display'] = '-' if display_oi == 0 else display_oi
                merged_contract['COI_display'] = '-' if display_coi == 0 else display_coi
                merged_contract['volume_display'] = '-' if display_volume == 0 else display_volume

                # Ensure expiryDate is always present and in DD-Mon-YYYY format
                merged_contract['expiryDate'] = merged_contract.get('expiryDate', nearest_expiry_display_format if nearest_expiry_display_format else 'N/A')
                
                enriched.append(merged_contract)
            return enriched

        calls_enriched = sorted(enrich_contracts(mostActiveCall_raw), key=lambda x: x.get('strikePrice', 0))
        puts_enriched = sorted(enrich_contracts(mostActivePut_raw), key=lambda x: x.get('strikePrice', 0))
        
        oi_enriched_temp = enrich_contracts(mostActiveOI_raw)
        
        index_contract = next((c for c in oi_enriched_temp if c.get('optionType') == 'XX'), None)
        oi_contracts_only = [c for c in oi_enriched_temp if c.get('optionType') != 'XX']
        
        oi_sorted = sorted(
            oi_contracts_only, 
            key=lambda x: -(pd.to_numeric(x.get('OI', 0), errors='coerce') or 0)
        )
        
        if index_contract:
            oi_sorted.insert(0, index_contract)
        
        # Now, `all_options_df_for_alerts` already contains the consolidated and cleaned data
        alerts_output = generate_alerts(pd.DataFrame([nifty_stats]), all_options_df_for_alerts)
        # Fetch Yahoo News separately
        news_fetcher = YahooNewsFetcher(limit=10)
        news = news_fetcher.get_news()


        return {
            'niftyStats': nifty_stats,
            'alerts': alerts_output,
            'mostActiveCalls': calls_enriched,
            'mostActivePuts': puts_enriched,
            'mostActiveOI': oi_sorted,
            "oc_json" : oc_json, # Keeping this for debugging/inspection
            "optionChainDetails": all_option_chain_details, # All option chain details for all strikes
            'news': news, 
            'spotprice': oc_json.get('records', {}).get('underlyingValue', 0.0),
            'bulckDealsSnapshot': get_large_deals_snapshot(s),
            'volumeGainers': get_volume_gainers(s),
            'mostActiveSecurities': get_most_active_securities(s),
            'corporatesPIT': get_corporates_pit(s),
        }

    except requests.exceptions.RequestException as req_err:
        print(f"Network error during data fetch: {req_err}")
        raise HTTPException(status_code=503, detail=f"Failed to fetch live data from NSE due to a network error: {req_err}")
    except Exception as e:
        print(f"An unexpected error occurred during live data processing: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred while processing data: {e}")