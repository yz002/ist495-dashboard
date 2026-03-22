import argparse
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def main():
    ap = argparse.ArgumentParser(description="Plot baseline same-day relationships.")
    ap.add_argument("--merged_csv", required=True, help="Path to merged baseline CSV")
    ap.add_argument("--min_total_posts", type=int, default=5)
    args = ap.parse_args()

    df = pd.read_csv(args.merged_csv)

    # Filter
    df = df[df["social_total_posts"] >= args.min_total_posts].copy()

    sns.set_style("whitegrid")

    # -------------------
    # 1) Density vs Change
    # -------------------
    plt.figure(figsize=(7,5))
    sns.regplot(
        x="message_density",
        y="Change_num",
        data=df,
        scatter_kws={"s":60},
        line_kws={"color":"red"}
    )
    plt.title("Message Density vs Same-Day Price Change")
    plt.xlabel("Message Density (posts/hour)")
    plt.ylabel("Price % Change")
    plt.tight_layout()
    plt.savefig("plot_density_vs_change.png", dpi=300)
    plt.show()

    # -------------------
    # 2) Sentiment vs Change
    # -------------------
    plt.figure(figsize=(7,5))
    sns.regplot(
        x="social_sentiment_score",
        y="Change_num",
        data=df,
        scatter_kws={"s":60},
        line_kws={"color":"red"}
    )
    plt.title("Sentiment Score vs Same-Day Price Change")
    plt.xlabel("Sentiment Score")
    plt.ylabel("Price % Change")
    plt.tight_layout()
    plt.savefig("plot_sentiment_vs_change.png", dpi=300)
    plt.show()

    # -------------------
    # 3) Weighted Density vs Change
    # -------------------
    plt.figure(figsize=(7,5))
    sns.regplot(
        x="weighted_density",
        y="Change_num",
        data=df,
        scatter_kws={"s":60},
        line_kws={"color":"red"}
    )
    plt.title("Weighted Density vs Same-Day Price Change")
    plt.xlabel("Weighted Density")
    plt.ylabel("Price % Change")
    plt.tight_layout()
    plt.savefig("plot_weighted_vs_change.png", dpi=300)
    plt.show()



if __name__ == "__main__":
    main()


