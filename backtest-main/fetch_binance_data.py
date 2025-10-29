import pandas as pd
import requests
from datetime import datetime, timedelta
import time
import sys

def fetch_binance_klines(symbol, interval, start_time=None, end_time=None, limit=1000):
    """
    Fetch historical klines/candlestick data from Binance API
    Returns only OHLC (no Volume)
    """
    url = "https://api.binance.com/api/v3/klines"
    
    params = {
        'symbol': symbol,
        'interval': interval,
        'limit': limit
    }
    
    if start_time:
        params['startTime'] = start_time
    if end_time:
        params['endTime'] = end_time
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        # Convert to DataFrame
        df = pd.DataFrame(data, columns=[
            'timestamp', 'Open', 'High', 'Low', 'Close', 'Volume',
            'close_time', 'quote_volume', 'trades', 'taker_buy_base',
            'taker_buy_quote', 'ignore'
        ])
        
        # Convert timestamp to datetime
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        
        # Convert price columns to float
        for col in ['Open', 'High', 'Low', 'Close']:
            df[col] = df[col].astype(float)
        
        # Keep only OHLC columns (no Volume)
        df = df[['timestamp', 'Open', 'High', 'Low', 'Close']]
        
        return df
    
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return None

def fetch_all_historical_data(symbol, interval, days_back):
    """
    Fetch all historical data by making multiple requests if needed
    """
    end_time = datetime.now()
    start_time = end_time - timedelta(days=days_back)
    
    start_ms = int(start_time.timestamp() * 1000)
    end_ms = int(end_time.timestamp() * 1000)
    
    interval_ms = {
        '1s': 1 * 1000,
        '1m': 60 * 1000,
        '5m': 5 * 60 * 1000,
        '15m': 15 * 60 * 1000,
        '1h': 60 * 60 * 1000,
        '4h': 4 * 60 * 60 * 1000,
        '1d': 24 * 60 * 60 * 1000,
    }
    
    if interval not in interval_ms:
        print(f"Unsupported interval: {interval}")
        return None
    
    candle_interval = interval_ms[interval]
    
    all_data = []
    current_start = start_ms
    
    print(f"Fetching {symbol} data from {start_time.strftime('%Y-%m-%d')} to {end_time.strftime('%Y-%m-%d')}")
    print(f"Interval: {interval}")
    print(f"Expected candles: ~{(end_ms - start_ms) // candle_interval}")
    print("-" * 60)
    
    request_count = 0
    
    while current_start < end_ms:
        df_batch = fetch_binance_klines(
            symbol=symbol,
            interval=interval,
            start_time=current_start,
            end_time=end_ms,
            limit=1000
        )
        
        if df_batch is None or len(df_batch) == 0:
            print("No more data available")
            break
        
        all_data.append(df_batch)
        request_count += 1
        
        last_timestamp = df_batch['timestamp'].iloc[-1]
        current_start = int(last_timestamp.timestamp() * 1000) + candle_interval
        
        print(f"Request {request_count}: Fetched {len(df_batch)} candles (up to {last_timestamp.strftime('%Y-%m-%d %H:%M')})")
        
        time.sleep(0.5)
        
        # Only break if we're at the most recent data AND have fewer than 1000 candles
        if len(df_batch) < 1000:
            # Check if we've reached current time (i.e., most recent candle)
            current_unix = int(datetime.now().timestamp() * 1000)
            last_timestamp_ms = int(last_timestamp.timestamp() * 1000)
            # If the last timestamp is very recent (within 1 hour of now), we've reached the end
            if current_unix - last_timestamp_ms < (60 * 60 * 1000):
                print("Reached the most recent data")
                break
            else:
                print(f"No more historical data available (stopped at {last_timestamp.strftime('%Y-%m-%d')})")
                break
    
    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        final_df = final_df.drop_duplicates(subset=['timestamp'])
        final_df = final_df.sort_values('timestamp').reset_index(drop=True)
        
        print("-" * 60)
        print(f"✓ Total candles fetched: {len(final_df)}")
        print(f"✓ Date range: {final_df['timestamp'].iloc[0]} to {final_df['timestamp'].iloc[-1]}")
        
        return final_df
    else:
        print("No data fetched")
        return None

def save_to_csv(df, filename):
    """Save DataFrame to CSV with OHLC only"""
    df_to_save = df.copy()
    df_to_save.set_index('timestamp', inplace=True)
    
    df_to_save.to_csv(filename)
    print(f"\n✓ Data saved to: {filename}")
    print(f"  Format: timestamp (index), Open, High, Low, Close")
    print(f"\nFirst 3 rows:")
    print(df_to_save.head(3))
    
    return filename

if __name__ == "__main__":
    SYMBOL = 'SOLUSDT'
    INTERVAL = '1h'
    DAYS_BACK = 365 * 2
    OUTPUT_FILE = f'sol_{INTERVAL}_data.csv'
    
    print("=" * 60)
    print("Binance Historical Data Fetcher (OHLC only)")
    print("=" * 60)
    
    df = fetch_all_historical_data(
        symbol=SYMBOL,
        interval=INTERVAL,
        days_back=DAYS_BACK
    )
    
    if df is not None:
        save_to_csv(df, OUTPUT_FILE)
        
        print("\n" + "=" * 60)
        print("Data Summary:")
        print("=" * 60)
        print(df.describe())
        
        print("\n✓ Success! Your data is ready for backtesting.")
    else:
        print("\n✗ Failed to fetch data.")
        sys.exit(1)
