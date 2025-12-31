# ---------------------------
#  1. Import dependencies
# ---------------------------
#%%
# First package to import and use before so it doesn't erase other imports

# Detect and handle Interactive or Non-Interactive mode
try:
    from IPython import get_ipython
    ipython = get_ipython()
    if ipython is not None:
        print("[Environment] Running in Interactive mode (IPython/Jupyter). Resetting workspace.")
        ipython.run_line_magic('reset', '-sf')
    else:
        print("[Environment] Running in Standard Python mode (non-interactive). No reset performed.")
except (ImportError, AttributeError):
    print("[Environment] Running in Standard Python mode (non-interactive). No reset performed.")

import pandas as pd
import os
import time
import random
from datetime import datetime
from pathlib import Path

# Selenium imports
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException



folder_path = Path(__file__).resolve().parents[1]

input_csv = folder_path / "data/spotify_playlists/main/liked.csv"      # change if you have a different working copy
output_csv = folder_path / "data/spotify_playlists/main/liked_yt_songstats.csv"   # keeps your original intact

# --- load ---
# Always resume from OUTPUT_CSV if it exists
csv_to_load = output_csv if os.path.exists(output_csv) else input_csv
df = pd.read_csv(csv_to_load, encoding="UTF-8")

BASE_SLEEP_SECONDS = 3.0            # seconds to wait for page to load
JITTER = 1.5                        # random extra seconds
SAVE_EVERY_N = 25                   # periodic checkpoint
TIMEOUT = 20

# Ensure required columns exist
for col in ["yt_url", "status"]:
    if col not in df.columns:
        df[col] = ""

# Normalize empties
df["yt_url"] = df["yt_url"].fillna("").astype(str)
df["status"] = df["status"].fillna("").astype(str)
if "isrc" in df.columns:
    df["isrc"] = df["isrc"].fillna("").astype(str).str.strip()
else:
    raise ValueError("No ISRC column found in your CSV.")

print(f"Loaded: {csv_to_load}")
print(f"Total rows: {len(df)}")

# Initialize Selenium WebDriver (headless)
chrome_options = Options()
chrome_options.add_argument("--headless")       # run without opening a visible browser window
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--disable-dev-shm-usage")
driver = webdriver.Chrome(options=chrome_options)

def fetch_youtube_link_via_selenium(isrc: str) -> str:
    """
    Open the Songstats page for the given ISRC using Selenium,
    wait for the dynamic content to load, and extract the YouTube link.
    Returns the YouTube URL or an empty string if not found.
    """
    url = f"https://songstats.com/{isrc}?ref=ISRCFinder"
    try:
        driver.get(url)
        # Wait a bit for SPA to load (implicit wait is optional here)
        # Using time.sleep plus jitter for a simple wait; adjust as needed
        wait_time = BASE_SLEEP_SECONDS + random.uniform(0, JITTER)
        time.sleep(wait_time)

        # Attempt to locate the YouTube link via CSS selector
        yt_elem = driver.find_element(By.CSS_SELECTOR, "a[href*='youtube.com/watch']")
        return yt_elem.get_attribute("href")
    except NoSuchElementException:
        # YouTube link not found
        return ""
    except Exception as e:
        # Other errors – could log if desired
        return ""

# --- resumable loop settings ---
MAX_ROWS_THIS_RUN = None   # set an int to limit a session, e.g. 200

processed = 0
updated = 0
start_time = time.time()

# Rows to work on: missing yt_url and not already done
todo_mask = (df["yt_url"].str.strip() == "") & (~df["status"].isin(["done"]))
todo_idx = df.index[todo_mask].tolist()

print(f"Rows total: {len(df)}")
print(f"Todo rows:  {len(todo_idx)}")

for n, i in enumerate(todo_idx, start=1):
    if MAX_ROWS_THIS_RUN is not None and processed >= MAX_ROWS_THIS_RUN:
        break

    isrc = df.at[i, "isrc"].strip()  # your CSV uses a lowercase column name here
    if not isrc:
        df.at[i, "status"] = "no_isrc"
        processed += 1
        continue

    yt_url = fetch_youtube_link_via_selenium(isrc)
    if yt_url:
        df.at[i, "yt_url"] = yt_url
        df.at[i, "status"] = "done"
        updated += 1
        # Save immediately on success (safest)
        df.to_csv(output_csv, index=False)
    else:
        # Mark as no YouTube link found
        df.at[i, "status"] = "no_yt"

    processed += 1

    # Periodic save
    if processed % SAVE_EVERY_N == 0:
        df.to_csv(output_csv, index=False)

print(f"Processed: {processed} | Updated yt_url: {updated} | Elapsed: {time.time() - start_time:.1f}s")

# Clean up
driver.quit()

# Save final result
df.to_csv(output_csv, index=False)
print(f"Saved to: {output_csv}")
