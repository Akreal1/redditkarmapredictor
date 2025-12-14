import time
import math
import requests
import pandas as pd
from datetime import datetime, timedelta

SUBREDDITS = [
    "AskReddit",
    "offmychest",
    "explainlikeimfive",
    "relationships",
    "relationship_advice",
    "AmItheAsshole",
    "personalfinance",
    "legaladvice",
    "AskScience",
    "AskHistorians",
]

MAX_TOTAL_POSTS = 10000
MIN_AGE_DAYS = 30
REQUESTS_PER_SECOND = 0.5  

OUTPUT_CSV = "reddit_text_karma_dataset.csv"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; karma-text-project/0.1; +https://example.com)"
}



def approx_votes(score, upvote_ratio):
    if score is None or upvote_ratio is None:
        return None, None

    if upvote_ratio <= 0.0 or upvote_ratio >= 1.0 or abs(2 * upvote_ratio - 1.0) < 1e-6:
        return None, None

    try:
        n = score / (2 * upvote_ratio - 1.0)
    except ZeroDivisionError:
        return None, None

    if n <= 0:
        return None, None

    u = upvote_ratio * n
    d = (1.0 - upvote_ratio) * n

    u = max(0, int(round(u)))
    d = max(0, int(round(d)))
    return u, d


def fetch_subreddit_posts(subreddit, max_posts_per_sub, min_created_utc, session):
    print(f"=== Fetching r/{subreddit} ===")
    collected = []
    after = None

    while len(collected) < max_posts_per_sub:
        url = f"https://www.reddit.com/r/{subreddit}/top.json"
        params = {
            "limit": 100,
            "t": "year",
        }
        if after:
            params["after"] = after

        try:
            resp = session.get(url, headers=HEADERS, params=params, timeout=15)
        except requests.RequestException as e:
            print(f"[{subreddit}] Request error: {e}")
            break

        if resp.status_code != 200:
            print(f"[{subreddit}] HTTP {resp.status_code}: {resp.text[:200]}")
            break

        data = resp.json().get("data", {})
        children = data.get("children", [])
        after = data.get("after")

        if not children:
            print(f"[{subreddit}] No more posts.")
            break

        for child in children:
            p = child.get("data", {})

            if not p.get("is_self", False):
                continue

            if p.get("over_18") or p.get("subreddit_over18"):
                continue

            created_utc = p.get("created_utc")
            if not isinstance(created_utc, (int, float)):
                continue

            if created_utc > min_created_utc:
                continue

            score = p.get("score", 0)
            upvote_ratio = p.get("upvote_ratio", None)

            ups_raw = p.get("ups", None)
            downs_raw = p.get("downs", None)

            ups_est, downs_est = approx_votes(score, upvote_ratio)

            row = {
                "id": p.get("id"),
                "subreddit": p.get("subreddit"),
                "title": p.get("title"),
                "selftext": p.get("selftext", "") or "",
                "score": score,
                "upvote_ratio": upvote_ratio,
                "ups_raw": ups_raw,
                "downs_raw": downs_raw,
                "ups_estimated": ups_est,
                "downs_estimated": downs_est,
                "num_comments": p.get("num_comments", 0),
                "created_utc": created_utc,
                "permalink": "https://www.reddit.com" + p.get("permalink", ""),
                "over_18": p.get("over_18", False),
                "is_self": p.get("is_self", False),
            }

            collected.append(row)

            if len(collected) >= max_posts_per_sub:
                break


        time.sleep(1.0 / REQUESTS_PER_SECOND)

        if not after:
            print(f"[{subreddit}] Reached end of listing.")
            break

    print(f"[{subreddit}] Collected {len(collected)} posts.")
    return collected



def main():
    min_created_dt = datetime.utcnow() - timedelta(days=MIN_AGE_DAYS)
    min_created_utc = min_created_dt.timestamp()

    max_per_sub = math.ceil(MAX_TOTAL_POSTS / len(SUBREDDITS))

    all_posts = []
    session = requests.Session()

    for sub in SUBREDDITS:
        if len(all_posts) >= MAX_TOTAL_POSTS:
            break

        remaining = MAX_TOTAL_POSTS - len(all_posts)
        per_sub_limit = min(max_per_sub, remaining)

        posts = fetch_subreddit_posts(
            subreddit=sub,
            max_posts_per_sub=per_sub_limit,
            min_created_utc=min_created_utc,
            session=session,
        )
        all_posts.extend(posts)

    if len(all_posts) > MAX_TOTAL_POSTS:
        all_posts = all_posts[:MAX_TOTAL_POSTS]

    print(f"\nTotal posts collected: {len(all_posts)}")

    if not all_posts:
        print("No data collected; exiting.")
        return

    df = pd.DataFrame(all_posts)

    df = df[(df["over_18"] == False) & (df["is_self"] == True)]

    df.to_csv(OUTPUT_CSV, index=False)
    print(f"Saved dataset to {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
