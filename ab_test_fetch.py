import time
import argparse
import statistics

# cloudscraper version
import cloudscraper

# curl-impersonate version
from curl_cffi import requests as creq

def fetch_cloudscraper(symbol: str):
    url = f"https://api.stocktwits.com/api/2/streams/symbol/{symbol}.json"
    s = cloudscraper.create_scraper()
    t0 = time.time()
    r = s.get(url, params={"limit": 50}, timeout=20)
    dt = time.time() - t0
    n = 0
    if r.status_code == 200:
        try:
            n = len(r.json().get("messages", []))
        except:
            n = 0
    return r.status_code, dt, n

def fetch_curl(symbol: str):
    url = f"https://api.stocktwits.com/api/2/streams/symbol/{symbol}.json"
    t0 = time.time()
    r = creq.get(
        url,
        params={"limit": 50},
        impersonate="chrome",
        timeout=20,
        headers={
            "accept": "application/json, text/plain, */*",
            "accept-language": "en-US,en;q=0.9",
            "origin": "https://stocktwits.com",
            "referer": f"https://stocktwits.com/symbol/{symbol}",
        },
    )
    dt = time.time() - t0
    n = 0
    if r.status_code == 200:
        try:
            n = len(r.json().get("messages", []))
        except:
            n = 0
    return r.status_code, dt, n

def run(method_name, fn, symbol, iters, pause):
    times = []
    statuses = []
    msg_counts = []
    print(f"\n=== {method_name} | symbol={symbol} | iters={iters} ===")
    for i in range(iters):
        try:
            status, dt, n = fn(symbol)
            statuses.append(status)
            times.append(dt)
            msg_counts.append(n)
            print(f"{i+1:02d}: status={status} time={dt:.3f}s msgs={n}")
        except Exception as e:
            print(f"{i+1:02d}: ERROR: {e}")
        time.sleep(pause)

    ok_times = [t for t, s in zip(times, statuses) if s == 200]
    print(f"\nSummary {method_name}:")
    if ok_times:
        print(f"- ok(200)={len(ok_times)}/{len(times)}")
        print(f"- avg_time_ok={statistics.mean(ok_times):.3f}s  median={statistics.median(ok_times):.3f}s")
    else:
        print(f"- ok(200)=0/{len(times)}")

    print(f"- status_counts={ {s: statuses.count(s) for s in sorted(set(statuses))} }")
    if msg_counts:
        print(f"- avg_msgs={statistics.mean(msg_counts):.1f}  max_msgs={max(msg_counts)}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("symbol")
    ap.add_argument("--iters", type=int, default=10)
    ap.add_argument("--pause", type=float, default=1.0)
    args = ap.parse_args()

    run("cloudscraper", fetch_cloudscraper, args.symbol, args.iters, args.pause)
    run("curl-impersonate", fetch_curl, args.symbol, args.iters, args.pause)

if __name__ == "__main__":
    main()