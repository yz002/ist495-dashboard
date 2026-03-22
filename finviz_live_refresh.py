import argparse
import subprocess
import time
import sys

def run_once(args):
    cmd = [
        sys.executable, "finviz_enricher.py",
        "--db", args.db,
        "--finviz_csv", args.finviz_csv,
        "--out_csv", args.out_csv,
        "--window_minutes", str(args.window_minutes),
        "--sort_by", args.sort_by,
    ]

    if args.desc:
        cmd.append("--desc")

    # Optional thresholds
    if args.min_sentiment is not None:
        cmd += ["--min_sentiment", str(args.min_sentiment)]
    if args.min_density is not None:
        cmd += ["--min_density", str(args.min_density)]

    print("\n=== REFRESH RUN ===")
    print("Command:", " ".join(cmd))
    subprocess.run(cmd, check=False)

def main():
    ap = argparse.ArgumentParser(description="Refresh finviz_enricher every N seconds.")
    ap.add_argument("--db", required=True)
    ap.add_argument("--finviz_csv", required=True)
    ap.add_argument("--out_csv", required=True)
    ap.add_argument("--window_minutes", type=int, default=120)
    ap.add_argument("--sort_by", default="weighted_density", choices=["density", "sentiment", "weighted_density"])
    ap.add_argument("--desc", action="store_true")
    ap.add_argument("--refresh_seconds", type=int, default=10)

    # thresholds optional
    ap.add_argument("--min_sentiment", type=float, default=None)
    ap.add_argument("--min_density", type=float, default=None)

    args = ap.parse_args()

    print("Starting live refresh loop...")
    print(f"refresh_seconds={args.refresh_seconds}, window_minutes={args.window_minutes}, sort_by={args.sort_by}, desc={args.desc}")
    if args.min_sentiment is not None or args.min_density is not None:
        print(f"thresholds: min_sentiment={args.min_sentiment}, min_density={args.min_density}")

    try:
        while True:
            run_once(args)
            time.sleep(args.refresh_seconds)
    except KeyboardInterrupt:
        print("\nStopped live refresh.")

if __name__ == "__main__":
    main()