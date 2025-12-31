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

import os
import time
import random
import re
import pandas as pd
from pathlib import Path
from bs4 import BeautifulSoup
from urllib.parse import urlparse, parse_qs

# Selenium imports
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException

# --- Configuration ---
# Set how many minutes you want the script to run.  After this time elapses,
# the loop will break gracefully.
MAX_RUNTIME_MINUTES = 1  # e.g. 30 minutes

# Paths
folder_path = Path(__file__).resolve().parents[1]
input_csv = folder_path / "data/spotify_playlists/main/liked.csv"
output_csv = folder_path / "data/spotify_playlists/main/liked_yt_songstats.csv"

# --- load ---
csv_to_load = output_csv if os.path.exists(output_csv) else input_csv
df = pd.read_csv(csv_to_load, encoding="UTF-8")

# Ensure required columns exist
for col in ["yt_url", "status"]:
    if col not in df.columns:
        df[col] = ""
df["yt_url"] = df["yt_url"].fillna("").astype(str)
df["status"] = df["status"].fillna("").astype(str)
if "isrc" in df.columns:
    df["isrc"] = df["isrc"].fillna("").astype(str).str.strip()
else:
    raise ValueError("No ISRC column found in your CSV.")

print(f"Loaded: {csv_to_load}")
print(f"Total rows: {len(df)}")
print(f"Script will run for up to {MAX_RUNTIME_MINUTES} minutes.\n")

# --- Selenium helper functions ---

def canonicalise_youtube_url(url: str) -> str:
    """Normalise YouTube URLs to https://www.youtube.com/watch?v=VIDEO_ID."""
    if not url:
        return url
    if url.startswith("//"):
        url = "https:" + url
    elif url.startswith("www."):
        url = "https://" + url
    elif url.startswith("http://"):
        url = url.replace("http://", "https://", 1)
    parsed = urlparse(url)
    netloc = parsed.netloc.lower()
    path = parsed.path
    query = parse_qs(parsed.query)
    # youtu.be short links
    if "youtu.be" in netloc and path:
        return f"https://www.youtube.com/watch?v={path.lstrip('/')}"
    if netloc.endswith("youtube.com"):
        if path == "/watch" and "v" in query:
            return f"https://www.youtube.com/watch?v={query['v'][0]}"
        if path.startswith(("/shorts/", "/embed/")):
            parts = path.strip("/").split("/")
            if len(parts) >= 2:
                return f"https://www.youtube.com/watch?v={parts[1]}"
        if len(path.strip("/")) == 11:
            return f"https://www.youtube.com/watch?v={path.strip('/')}"
    return url

def extract_youtube_from_soup(soup: BeautifulSoup) -> str | None:
    """Extract the YouTube link from a BeautifulSoup object and return the canonical URL."""
    links_label = soup.find(
        lambda tag: tag.name in ("span", "div")
        and tag.get_text(strip=True).lower().startswith("links")
    )
    anchors = []
    if links_label:
        container = links_label
        for _ in range(4):
            if container.find_all("a", href=True):
                anchors = container.find_all("a", href=True)
                break
            container = container.parent
    # Search anchors by aria-label or href
    for a in anchors:
        aria = (a.get("aria-label") or "").lower()
        href = a.get("href", "").strip()
        if "youtube" in aria and ("youtube.com" in href or "youtu.be" in href):
            return canonicalise_youtube_url(href)
    for a in anchors:
        href = a.get("href", "").strip()
        if "youtube.com" in href or "youtu.be" in href:
            return canonicalise_youtube_url(href)
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if "youtube.com" in href or "youtu.be" in href:
            return canonicalise_youtube_url(href)
    # Last resort: regex scan of the text
    text = soup.get_text(" ", strip=True)
    m = re.search(r"https?://(?:www\.)?(?:youtube\.com|youtu\.be)/[^\s\"'<>]+", text, re.I)
    if m:
        return canonicalise_youtube_url(m.group(0))
    return None

def fetch_youtube_via_selenium(isrc: str, driver: webdriver.Remote,
                               load_wait: int = 7, redirect_timeout: int = 15) -> str | None:
    """Render the Songstats page for an ISRC in Selenium, wait for the Links block, and extract the YouTube URL."""
    base_url = f"https://songstats.com/{isrc}?ref=ISRCFinder"
    try:
        driver.get(base_url)
        # Wait for a client-side redirect to complete
        try:
            WebDriverWait(driver, redirect_timeout).until(
                lambda d: d.current_url != base_url
            )
        except TimeoutException:
            pass
        # Wait for a potential YouTube element; ignore TimeoutException
        try:
            WebDriverWait(driver, load_wait).until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR,
                     "a[aria-label*='youtube'], a[href*='youtube.com'], a[href*='youtu.be']")
                )
            )
        except TimeoutException:
            pass
        soup = BeautifulSoup(driver.page_source, "html.parser")
        return extract_youtube_from_soup(soup)
    except WebDriverException as e:
        print(f"WebDriver error while processing {isrc}: {e}")
        return None

# --- Selenium setup (headless) ---
options = webdriver.FirefoxOptions()
#options.add_argument("--headless")
driver = webdriver.Firefox(options=options)

# --- resumable loop settings ---
SAVE_EVERY_N = 25      # periodic checkpoint
BASE_SLEEP_SECONDS = 3  # polite delay with jitter
MAX_ROWS_THIS_RUN = None  # optional limiter

processed = 0
updated = 0
start_time = time.time()
max_runtime_seconds = MAX_RUNTIME_MINUTES * 60

todo_mask = (df["yt_url"].str.strip() == "") & (~df["status"].isin(["done"]))
todo_idx = df.index[todo_mask].tolist()

total_todo = len(todo_idx)
print(f"Rows total: {len(df)}")
print(f"Todo rows:  {total_todo}\n")

for n, i in enumerate(todo_idx, start=1):
    # Check time limit
    elapsed = time.time() - start_time
    if elapsed > max_runtime_seconds:
        print(f"Maximum runtime of {MAX_RUNTIME_MINUTES} minutes reached. Stopping gracefully.")
        break

    if MAX_ROWS_THIS_RUN is not None and processed >= MAX_ROWS_THIS_RUN:
        break

    isrc = df.at[i, "isrc"].strip()
    print(f"[{n}/{total_todo}] Processing index {i}, ISRC: '{isrc}'")

    if not isrc:
        df.at[i, "status"] = "no_isrc"
        processed += 1
        print(" -> No ISRC found. Marked as 'no_isrc'.")
        continue

    try:
        yt_url = fetch_youtube_via_selenium(isrc, driver, load_wait=7, redirect_timeout=15)
        if yt_url:
            df.at[i, "yt_url"] = yt_url
            df.at[i, "status"] = "done"
            updated += 1
            print(f" -> Found YouTube URL: {yt_url}")
        else:
            df.at[i, "status"] = "no_yt"
            print(" -> No YouTube link found on the page.")

        # Save after each successful or attempted fetch
        df.to_csv(output_csv, index=False)
    except Exception as e:
        df.at[i, "status"] = "error"
        print(f" -> Exception occurred while processing {isrc}: {e}")

    processed += 1

    if processed % SAVE_EVERY_N == 0:
        df.to_csv(output_csv, index=False)
        print(f"Checkpoint: saved after processing {processed} rows.")

    # Polite delay with jitter to reduce the chance of being rate-limited
    time.sleep(BASE_SLEEP_SECONDS + random.uniform(0, 1.0))

overall_elapsed = time.time() - start_time
print(f"\nFinished run. Processed rows: {processed}, Updated yt_url: {updated}, "
      f"Elapsed time: {overall_elapsed / 60:.2f} minutes")

driver.quit()
df.to_csv(output_csv, index=False)
print(f"Data saved to: {output_csv}")

#%%