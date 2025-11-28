from fastapi import FastAPI, HTTPException
import yfinance as yf
import pandas as pd
import time

app = FastAPI()

# ---------------------------------------------------------
# CACHING (critical to avoid rate limits)
# ---------------------------------------------------------
CACHE = {}
CACHE_TTL = 15  # seconds (safe for "live" feeling without hitting limits)

def get_cache(key):
    if key in CACHE:
        data, ts = CACHE[key]
        if time.time() - ts < CACHE_TTL:
            return data
    return None

def set_cache(key, data):
    CACHE[key] = (data, time.time())


# ---------------------------------------------------------
# TIMEFRAMES
# ---------------------------------------------------------
TIMEFRAMES = {
    "1d": ("1d", "30m"),
    "1w": ("7d", "1d"),
    "1mo": ("1mo", "1d"),
    "6mo": ("6mo", "1d"),
    "1y": ("1y", "1d"),
    "5y": ("5y", "1wk"),
    "all": ("max", "1mo")
}


# ---------------------------------------------------------
# /stock/{ticker} — SAFE VERSION
# ---------------------------------------------------------
@app.get("/stock/{ticker}")
def get_stock(ticker: str, period: str = "1mo"):

    # Cache key
    cache_key = f"stock:{ticker}:{period}"
    cached = get_cache(cache_key)
    if cached:
        return cached

    period = period.lower()

    if period not in TIMEFRAMES:
        raise HTTPException(400, f"Invalid period: {list(TIMEFRAMES.keys())}")

    period_value, interval = TIMEFRAMES[period]

    try:
        # ONE LIGHTWEIGHT CALL
        df = yf.download(
            tickers=ticker,
            period=period_value,
            interval=interval,
            progress=False,
            threads=False
        )

        if df.empty:
            raise HTTPException(404, f"No data found for {ticker.upper()}")

        # Fix multiindex
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [c[0] for c in df.columns]

        df = df.reset_index()

        # Fix datetime column
        if "Datetime" in df.columns:
            df.rename(columns={"Datetime": "datetime"}, inplace=True)
        elif "Date" in df.columns:
            df.rename(columns={"Date": "datetime"}, inplace=True)
        else:
            raise HTTPException(500, "Missing datetime column")

        df["datetime"] = df["datetime"].dt.strftime("%Y-%m-%d %H:%M:%S")

        df = df[["datetime", "Open", "High", "Low", "Close", "Volume"]]

        latest = df.iloc[-1]

        # LIGHTWEIGHT FUNDAMENTALS
        tkr = yf.Ticker(ticker)
        fast = tkr.fast_info  # MUCH safer than info

        metrics = {
            "open": float(latest["Open"]),
            "high": float(latest["High"]),
            "low": float(latest["Low"]),
            "close": float(latest["Close"]),
            "volume": int(latest["Volume"]) if not pd.isna(latest["Volume"]) else None,

            "52w_high": fast.get("yearHigh"),
            "52w_low": fast.get("yearLow"),
            "market_cap": fast.get("marketCap"),
            "pe_ratio": fast.get("trailingPE"),
            "avg_volume": fast.get("tenDayAverageVolume"),
            "beta": fast.get("beta"),
        }

        response = {
            "ticker": ticker.upper(),
            "period": period,
            "interval": interval,
            "count": len(df),
            "metrics": metrics,
            "data": df.to_dict(orient="records")
        }

        set_cache(cache_key, response)
        return response

    except Exception as e:
        raise HTTPException(500, f"Error fetching data: {str(e)}")


# ---------------------------------------------------------
# POPULAR TICKERS
# ---------------------------------------------------------
NASDAQ = ["NVDA", "AAPL", "MSFT", "GOOGL", "AMZN", "AVGO", "META", "TSLA", "ASML", "INTC"]
SENSEX = ["INFY.NS", "RELIANCE.NS", "TECHM.NS", "TCS.NS", "BHARTIARTL.NS",
          "TATASTEEL.NS", "TATAMOTORS.NS", "HCLTECH.NS", "HDFCBANK.NS", "BEL.NS"]
DOW = ["MMM", "AAPL", "JNJ", "V", "UNH", "JPM", "PG", "HD", "IBM", "KO"]

MARKET_TICKERS = {"nasdaq": NASDAQ, "sensex": SENSEX, "dow": DOW}


# ---------------------------------------------------------
# /live/bulk/{market} — SAFE BATCH VERSION
# ---------------------------------------------------------
@app.get("/live/bulk/{market}")
def get_bulk(market: str):

    market = market.lower()
    if market not in MARKET_TICKERS:
        return {"error": "Invalid. Use nasdaq, sensex, dow"}

    tickers = MARKET_TICKERS[market]

    cache_key = f"bulk:{market}"
    cached = get_cache(cache_key)
    if cached:
        return cached

    # ONE BATCH REQUEST (instead of 10 separate calls)
    ticker_str = " ".join(tickers)

    df = yf.download(
        tickers=ticker_str,
        period="3d",
        interval="1d",
        group_by="ticker",
        progress=False
    )

    results = []

    for t in tickers:
        try:
            tdf = df[t]

            if len(tdf) < 2:
                results.append({
                    "ticker": t,
                    "yesterday_close": None,
                    "change": None,
                    "yesterday_date": None
                })
                continue

            yesterday_close = float(tdf["Close"].iloc[-2])
            yesterday_date = tdf.index[-2].strftime("%Y-%m-%d")

            if len(tdf) >= 3:
                prev_close = float(tdf["Close"].iloc[-3])
                change = yesterday_close - prev_close
            else:
                change = None

            results.append({
                "ticker": t,
                "yesterday_close": yesterday_close,
                "change": round(change, 2) if change else None,
                "yesterday_date": yesterday_date
            })

        except Exception:
            results.append({
                "ticker": t,
                "yesterday_close": None,
                "change": None,
                "yesterday_date": None
            })

    set_cache(cache_key, results)
    return results




