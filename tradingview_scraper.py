import requests
import json
import argparse
import time
import os
from datetime import datetime

def get_price_data(symbol, asset_type="stock"):
    """
    Fetches real-time price data for a symbol from TradingView Scanner.
    asset_type: 'stock' (uses america scanner) or 'crypto' (uses crypto scanner)
    """
    if asset_type == "crypto":
        url = "https://scanner.tradingview.com/crypto/scan"
        # Common crypto exchanges
        exchanges = ["BINANCE", "COINBASE", "KRAKEN", "BITSTAMP"]
    else:
        url = "https://scanner.tradingview.com/america/scan" 
        exchanges = ["NASDAQ", "NYSE", "AMEX"]

    # Try the symbol as provided first
    candidates = [symbol]
    if ":" not in symbol:
        for ex in exchanges:
            candidates.append(f"{ex}:{symbol}")
        
    for ticker in candidates:
        payload = {
            "symbols": {
                "tickers": [ticker]
            },
            "columns": ["close", "volume", "change", "change_abs"]
        }
        
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36"
        }

        try:
            response = requests.post(url, json=payload, headers=headers, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if data and data.get("data"):
                    # Found it!
                    return data["data"][0]["d"] # Returns list [close, volume, change, change_abs]
                # If data is empty, loop to next candidate
            else:
                print(f"Error {response.status_code}: {response.text}")
        except Exception as e:
            print(f"Request Error: {e}")
            
    # If we get here, no candidate worked
    return None

def main():
    parser = argparse.ArgumentParser(description="Scrape TradingView real-time price data.")
    parser.add_argument("symbol", type=str, help="The stock/crypto symbol (e.g., NASDAQ:AAPL or BINANCE:BTCUSDT)")
    parser.add_argument("--type", type=str, choices=['stock', 'crypto'], default='stock', help="Asset type: stock (default) or crypto")
    parser.add_argument("--output", type=str, help="Output JSON filename (optional)")
    parser.add_argument("--interval", type=float, default=1.0, help="Polling interval in seconds (default: 1.0)")
    
    args = parser.parse_args()
    output_filename = args.output if args.output else f"{args.symbol.replace(':', '_')}_price.json"
    
    existing_data = []
    
    # Load state
    if os.path.exists(output_filename):
        try:
            with open(output_filename, 'r', encoding='utf-8') as f:
                existing_data = json.load(f)
                print(f"Loaded {len(existing_data)} existing records.")
        except Exception as e:
            print(f"Error loading file: {e}")

    print(f"Starting TradingView Price Scraper for {args.symbol}.")
    print(f"Saving to {output_filename}. Polling every {args.interval} seconds.")
    
    try:
        while True:
            timestamp = datetime.now().isoformat()
            
            raw_data = get_price_data(args.symbol, asset_type=args.type)
            
            if raw_data:
                # raw_data is [close, volume, change, change_abs]
                price = raw_data[0]
                volume = raw_data[1]
                change_percent = raw_data[2]
                change_abs = raw_data[3]
                
                record = {
                    "time": timestamp,
                    "price": price,
                    "volume": volume,
                    "change_percent": change_percent,
                    "change_abs": change_abs
                }
                
                existing_data.append(record)
                
                # Output to console
                print(f"[{timestamp}] Price: {price} | Vol: {volume}")
                
                # Save immediately (or we could buffer, but safety first for now)
                try:
                    with open(output_filename, 'w', encoding='utf-8') as f:
                        json.dump(existing_data, f, indent=4)
                except Exception as e:
                    print(f"Save error: {e}")
            
            else:
                print(f"[{timestamp}] No data found or error.")

            time.sleep(args.interval)

    except KeyboardInterrupt:
        print("\nStopped.")
    except Exception as e:
        print(f"Critical error: {e}")

if __name__ == "__main__":
    main()
