import json
import sys
import time
import yfinance as yf

TICKERS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA",
    "META", "TSLA", "ORCL", "MSTR", "NFLX"
]


def fetch_ticker_info_with_retry(symbol, max_retries=10, base_delay=0.5):

    for attempt in range(1, max_retries + 1):
        try:
            info = yf.Ticker(symbol).info
            if info:
                return info
            else:
                raise ValueError("Empty info dict")
        except Exception as e:
            if attempt < max_retries:
                time.sleep(base_delay * attempt)

    return {}


def fetch_ohlcv(tickers):

    df = yf.download(
        tickers=tickers,
        period="1d",
        interval="5m",
        group_by="ticker",
        auto_adjust=False,
        progress=False,
    )

    result = []

    available_symbols = list(df.columns.levels[0])

    for symbol in tickers:
        if symbol not in available_symbols:
            continue

        sub = df[symbol].dropna()
        if sub.empty:
            continue

        last_row = sub.iloc[-1]
        idx = sub.index[-1]

        if hasattr(idx, "to_pydatetime"):
            ts = idx.to_pydatetime().replace(microsecond=0)
        else:
            ts = idx

        info = fetch_ticker_info_with_retry(symbol)
        name = info.get("shortName") or info.get("longName") or None

        result.append(
            {
                "symbol": symbol,
                "name": name,
                "datetime": ts.isoformat(),
                "open": float(last_row["Open"]),
                "high": float(last_row["High"]),
                "low": float(last_row["Low"]),
                "close": float(last_row["Close"]),
                "volume": int(last_row["Volume"]),
            }
        )

    return result


def main():
    data = fetch_ohlcv(TICKERS)
    json.dump(data, sys.stdout, ensure_ascii=False)
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
