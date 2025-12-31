#%%
"""
This script integrates Discogs API search into the existing Songstats scraping workflow.

It loads a CSV of Spotify liked tracks and attempts to fill missing YouTube links.
For each album without a YouTube link found via Songstats, the script queries the Discogs
database API to retrieve any associated YouTube videos.  It then matches each video
title to track names in that album (case‑insensitive substring match) and writes
matched YouTube URLs back into the CSV.  Any videos that cannot be matched to
existing tracks are recorded in a separate CSV for later review as potential new songs.

The code is designed to be resumable and polite: it respects Discogs rate limits
by sleeping between API calls and prints informative messages to track progress.
"""

import os
import time
import pandas as pd
import re
from pathlib import Path
from typing import List, Tuple, Dict

import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

###############################################################################
# Discogs API search functions
###############################################################################

# Load Discogs API credentials from environment variables
CONSUMER_KEY = os.getenv("DISCOGS_CONSUMER_KEY")
CONSUMER_SECRET = os.getenv("DISCOGS_CONSUMER_SECRET")

# Number of results to inspect from each search call.  A small number keeps
# requests quick and helps avoid irrelevant matches.
DISCOGS_SEARCH_LIMIT = 3

# Seconds to pause before every network request.  Discogs throttles unauthenticated
# requests to ~25 per minute.  A delay of 1.2s ensures we never exceed this.
API_CALL_DELAY = 1.2


def _sleep():
    """Helper to enforce the global API call delay."""
    time.sleep(API_CALL_DELAY)


def search_album_discogs(artist: str, title: str, key: str, secret: str) -> Dict:
    """
    Search Discogs for master releases by separate artist and release_title fields.
    Only master records are returned.  Returns the JSON response.
    """
    _sleep()
    base_url = "https://api.discogs.com/database/search"
    params = {
        "artist": artist,
        "release_title": title,
        "type": "master",
        "per_page": DISCOGS_SEARCH_LIMIT,
        "page": 1,
        "key": key,
        "secret": secret,
    }
    headers = {"User-Agent": "Happyapi/1.0 (loic.lahellec@dauphine.eu)"}
    print(f"[Fielded search] artist='{artist}', release_title='{title}'")
    resp = requests.get(base_url, params=params, headers=headers)
    resp.raise_for_status()
    return resp.json()


def search_album_combined(artist: str, title: str, key: str, secret: str) -> Dict:
    """
    Search Discogs using a combined 'Artist - Title' string.  Only master
    records are returned.  Returns the JSON response.
    """
    _sleep()
    base_url = "https://api.discogs.com/database/search"
    combined = f"{artist} - {title}"
    params = {
        "title": combined,
        "type": "master",
        "per_page": DISCOGS_SEARCH_LIMIT,
        "page": 1,
        "key": key,
        "secret": secret,
    }
    headers = {"User-Agent": "Happyapi/1.0 (loic.lahellec@dauphine.eu)"}
    print(f"[Combined search] title='{combined}'")
    resp = requests.get(base_url, params=params, headers=headers)
    resp.raise_for_status()
    return resp.json()


def search_album_release(artist: str, title: str, key: str, secret: str) -> Dict:
    """
    Search Discogs by artist and release_title but WITHOUT filtering to master records.
    Useful when only a release exists and there is no master.  Returns the JSON response.
    """
    _sleep()
    base_url = "https://api.discogs.com/database/search"
    params = {
        "artist": artist,
        "release_title": title,
        "per_page": DISCOGS_SEARCH_LIMIT,
        "page": 1,
        "key": key,
        "secret": secret,
    }
    headers = {"User-Agent": "Happyapi/1.0 (loic.lahellec@dauphine.eu)"}
    print(f"[Release search] artist='{artist}', release_title='{title}' (no type filter)")
    resp = requests.get(base_url, params=params, headers=headers)
    resp.raise_for_status()
    return resp.json()


def search_master_by_query(query: str, key: str, secret: str, per_page: int = 1) -> Dict:
    """
    Perform a free‑form query search.  Only master records are returned.
    Returns the JSON response.
    """
    _sleep()
    base_url = "https://api.discogs.com/database/search"
    params = {
        "q": query,
        "type": "master",
        "per_page": per_page,
        "page": 1,
        "key": key,
        "secret": secret,
    }
    headers = {"User-Agent": "Happyapi/1.0 (loic.lahellec@dauphine.eu)"}
    print(f"[Query search] q='{query}'")
    resp = requests.get(base_url, params=params, headers=headers)
    resp.raise_for_status()
    return resp.json()


def fetch_master_videos(master_url: str, key: str, secret: str) -> List[Tuple[str, str]]:
    """
    Given a master API URL, return a list of (video_title, video_uri) tuples.
    On error, returns an empty list.  Prints diagnostic messages about progress.
    """
    _sleep()
    headers = {"User-Agent": "Happyapi/1.0 (loic.lahellec@dauphine.eu)"}
    params = {"key": key, "secret": secret}
    try:
        print(f"    -> Fetching master: {master_url}")
        resp = requests.get(master_url, params=params, headers=headers)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"    ! Failed to fetch master {master_url}: {e}")
        return []
    videos = [
        (v.get("title", ""), v.get("uri"))
        for v in data.get("videos", [])
        if v.get("title") and v.get("uri")
    ]
    print(f"    -> {len(videos)} video(s) found on master")
    return videos


def fetch_release_videos(release_url: str, key: str, secret: str) -> List[Tuple[str, str]]:
    """
    Given a release API URL, return a list of (video_title, video_uri) tuples.
    On error, returns an empty list.
    """
    _sleep()
    headers = {"User-Agent": "Happyapi/1.0 (loic.lahellec@dauphine.eu)"}
    params = {"key": key, "secret": secret}
    try:
        print(f"    -> Fetching release: {release_url}")
        resp = requests.get(release_url, params=params, headers=headers)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"    ! Failed to fetch release {release_url}: {e}")
        return []
    videos = [
        (v.get("title", ""), v.get("uri"))
        for v in data.get("videos", [])
        if v.get("title") and v.get("uri")
    ]
    print(f"    -> {len(videos)} video(s) found on release")
    return videos


def try_result_list(results: List[Dict], key: str, secret: str) -> List[Tuple[str, str]]:
    """
    Iterate over search results and attempt to fetch videos from master or release
    endpoints.  Returns the first non‑empty list of videos encountered.
    """
    for idx, result in enumerate(results, 1):
        master_url = result.get("master_url")
        master_id = result.get("master_id")
        resource_url = result.get("resource_url")
        print(
            f"  [Candidate {idx}] title='{result.get('title')}', country='{result.get('country')}', year='{result.get('year')}'"
        )
        videos: List[Tuple[str, str]] = []
        # Try the master URL first
        if master_url:
            videos = fetch_master_videos(master_url, key, secret)
        # If no videos but a master_id exists, construct the master URL
        if not videos and master_id:
            constructed = f"https://api.discogs.com/masters/{master_id}"
            print("    -> No master_url, using master_id")
            videos = fetch_master_videos(constructed, key, secret)
        # Fallback to the release's resource URL
        if not videos and resource_url:
            print("    -> Falling back to release resource_url")
            videos = fetch_release_videos(resource_url, key, secret)
        if videos:
            return videos
    return []


def get_album_youtube_videos(artist: str, album: str, key: str, secret: str) -> List[Tuple[str, str]]:
    """
    Try to find YouTube videos for an album.  The search proceeds through four
    strategies in order of decreasing specificity:
      1) Fielded master search (artist + release_title)
      2) Combined master search ("Artist - Title")
      3) Release search without master filter
      4) General query master search (q parameter)
    The first non‑empty list of videos found is returned.  If none are
    found, an empty list is returned.
    """
    # Strategy 1: Fielded search (masters only)
    try:
        search = search_album_discogs(artist, album, key, secret)
        results = search.get("results", [])
        print(f"[Fielded search] {len(results)} result(s) returned")
        videos = try_result_list(results, key, secret)
        if videos:
            print("[Fielded search] Found videos, stopping")
            return videos
    except Exception as e:
        print(f"[Fielded search] Error: {e}")

    # Strategy 2: Combined search (masters only)
    try:
        search = search_album_combined(artist, album, key, secret)
        results = search.get("results", [])
        print(f"[Combined search] {len(results)} result(s) returned")
        videos = try_result_list(results, key, secret)
        if videos:
            print("[Combined search] Found videos, stopping")
            return videos
    except Exception as e:
        print(f"[Combined search] Error: {e}")

    # Strategy 3: Release search (no master filter)
    try:
        search = search_album_release(artist, album, key, secret)
        results = search.get("results", [])
        print(f"[Release search] {len(results)} result(s) returned")
        videos = try_result_list(results, key, secret)
        if videos:
            print("[Release search] Found videos, stopping")
            return videos
    except Exception as e:
        print(f"[Release search] Error: {e}")

    # Strategy 4: Query search (masters only)
    try:
        query = f"{artist} {album}"
        search = search_master_by_query(query, key, secret, per_page=DISCOGS_SEARCH_LIMIT)
        results = search.get("results", [])
        print(f"[Query search] {len(results)} result(s) returned")
        videos = try_result_list(results, key, secret)
        if videos:
            print("[Query search] Found videos, stopping")
            return videos
    except Exception as e:
        print(f"[Query search] Error: {e}")

    print("No videos found after all search methods.")
    return []


###############################################################################
# Data matching and update logic
###############################################################################

def normalise_string(s: str) -> str:
    """
    Lowercase a string and remove non‑alphanumeric characters for loose matching.
    Multiple spaces are collapsed.  Useful to compare track titles to video titles.
    """
    # Replace dashes and underscores with spaces
    s = s.lower()
    s = re.sub(r"[\-_]", " ", s)
    # Remove everything that's not a letter, number or space
    s = re.sub(r"[^a-z0-9\s]", "", s)
    # Collapse multiple spaces
    s = re.sub(r"\s+", " ", s).strip()
    return s


def match_videos_to_tracks(videos: List[Tuple[str, str]], track_names: List[str]) -> Dict[int, Tuple[str, str]]:
    """
    Given a list of (video_title, video_url) and a list of track names, return a
    dictionary mapping track indices to the first matching video (title, url).
    A match is considered valid if the normalised track name is a substring of
    the normalised video title.  Track indices correspond to positions in
    track_names.
    """
    matched = {}
    # Precompute normalised track names
    norm_tracks = [normalise_string(t) for t in track_names]
    for vid_title, vid_url in videos:
        norm_title = normalise_string(vid_title)
        for idx, norm_track in enumerate(norm_tracks):
            if idx in matched:
                continue  # skip already matched tracks
            # Accept a match if the track name is contained in the video title
            # or the video title is contained in the track name. This allows
            # matching cases like "song name - remastered" (track) with
            # "song name" (video) and vice versa.
            if norm_track and (norm_track in norm_title or norm_title in norm_track):
                matched[idx] = (vid_title, vid_url)
                # Break to avoid matching multiple tracks to the same video
                break
    return matched


###############################################################################
# Main album iteration routine
###############################################################################

def update_yt_links_with_discogs(csv_path: str, output_path: str, key: str, secret: str,
                                 max_runtime_minutes: int = 60, save_every_n: int = 5) -> None:
    """
    Iterate through a CSV of Spotify track data album by album and populate missing
    YouTube URLs using the Discogs API.  Tracks that already have a YouTube URL
    (status == 'done') are skipped.  Only tracks missing yt_url are processed.
    If no video is found for an album, each missing row is marked with status
    'no_yt'.  If videos are found, rows with matching track names are updated
    with the YouTube URL and 'yt_url_origin' set to 'discogs'.  Any extra
    videos from Discogs that do not correspond to a track are saved to a
    separate CSV called 'new_songs_from_discogs.csv'.

    Args:
        csv_path: Path to the input CSV file.
        output_path: Path where the updated CSV should be saved.
        key: Discogs consumer key.
        secret: Discogs consumer secret.
        max_runtime_minutes: Maximum number of minutes the script should run.
        save_every_n: Save progress to disk after processing every n albums.
    """
    df = pd.read_csv(csv_path, encoding="UTF-8")

    # Ensure necessary columns exist
    for col in ["yt_url", "status", "yt_url_origin"]:
        if col not in df.columns:
            df[col] = ""
    df["yt_url"] = df["yt_url"].fillna("").astype(str)
    df["status"] = df["status"].fillna("").astype(str)
    df["yt_url_origin"] = df["yt_url_origin"].fillna("").astype(str)

    # Identify albums (combination of album name and album artist) to process
    album_cols = ["album_name", "album_artist_name(s)"]
    if not all(col in df.columns for col in album_cols):
        raise ValueError("CSV must contain 'album_name' and 'album_artist_name(s)' columns.")

    # Determine rows needing processing: missing yt_url and status not in ['done', 'no_yt']
    to_process_mask = (df["yt_url"].str.strip() == "") & (~df["status"].isin(["done", "no_yt"]))
    to_process = df[to_process_mask].index.tolist()
    # Group by album and album artist.  Preserve order of appearance by disabling sort.
    groups = df.loc[to_process].groupby(album_cols, sort=False)

    # Prepare container for unmatched videos
    extra_rows: List[Dict[str, str]] = []

    processed_albums = 0
    start_time = time.time()
    max_seconds = max_runtime_minutes * 60
    print(f"Starting Discogs update: {len(groups)} album(s) to process.")

    # iterate through each album group; idxs is a DataFrame containing the rows for this album
    for (album_name, album_artist), idxs in groups:
        # Check runtime limit
        if time.time() - start_time > max_seconds:
            print("Maximum runtime reached, stopping gracefully.")
            break
        processed_albums += 1
        print(f"\nProcessing album {processed_albums}/{len(groups)}: '{album_name}' by '{album_artist}'")

        # idxs is a DataFrame; use its index to locate rows
        idx_list = idxs.index.tolist()
        # Skip if this album already processed (i.e., all tracks have yt_url)
        if df.loc[idx_list, "yt_url"].str.strip().ne("").all():
            print(" -> All tracks already have YouTube URLs, skipping.")
            continue

        # Retrieve track names for matching
        track_names = df.loc[idx_list, "track_name"].tolist()

        # Perform Discogs search
        videos = get_album_youtube_videos(album_artist, album_name, key, secret)

        if not videos:
            # Mark all missing rows as no_yt
            print(f" -> No videos found for album '{album_name}', marking tracks as no_yt.")
            for row_idx in idx_list:
                if df.at[row_idx, "yt_url"].strip() == "":
                    df.at[row_idx, "status"] = "no_yt"
            # Save progress after marking no_yt for this album
        else:
            # Match videos to tracks
            match_map = match_videos_to_tracks(videos, track_names)
            print(f" -> Matched {len(match_map)} of {len(track_names)} tracks.")
            # Update DataFrame with matched videos
            for local_idx, (video_title, video_url) in match_map.items():
                row_idx = idx_list[local_idx]
                if df.at[row_idx, "yt_url"].strip() == "":
                    df.at[row_idx, "yt_url"] = video_url
                    df.at[row_idx, "yt_url_origin"] = "discogs"
                    df.at[row_idx, "status"] = "done"
            # Any unmatched videos are considered new songs
            for vid_title, vid_url in videos:
                # If this video wasn't matched to any track, record it
                if (vid_title, vid_url) not in match_map.values():
                    extra_rows.append(
                        {
                            "album_name": album_name,
                            "album_artist": album_artist,
                            "video_title": vid_title,
                            "yt_url": vid_url,
                        }
                    )
            # Mark any remaining unmatched album tracks as no_yt
            for local_idx, row_idx in enumerate(idx_list):
                if df.at[row_idx, "yt_url"].strip() == "":
                    df.at[row_idx, "status"] = "no_yt"

        # Periodically save progress
        if processed_albums % save_every_n == 0:
            df.to_csv(output_path, index=False)
            print(f" -> Progress saved after {processed_albums} albums.")

    # Save final data
    df.to_csv(output_path, index=False)
    print(f"Finished Discogs update. Processed {processed_albums} albums.\nData saved to {output_path}")

    # Save extra songs if any
    if extra_rows:
        extra_df = pd.DataFrame(extra_rows)
        extra_path = Path(output_path).with_name("new_songs_from_discogs.csv")
        if extra_df is not None:
            extra_df.to_csv(extra_path, index=False)
            print(f"Extra songs from Discogs saved to {extra_path}")
        else:
            print("No extra songs to save.")


# -----------------------------------------------------------------------------
# Automatic initialisation
#
# In keeping with the original Songstats script, this module executes the
# Discogs update when imported or run directly.  It determines the input
# and output CSV paths relative to the project root and then invokes
# update_yt_links_with_discogs().  You can comment out the lines below if you
# prefer to call the update function manually from another script.
data_folder = Path(__file__).resolve().parents[1] / "data/spotify_playlists/main"
# Use the same CSV for input and output so the script progresses on a single file
csv_path = data_folder / "liked_yt_discogs.csv"

print("\nStarting Discogs scraping step…")
update_yt_links_with_discogs(
    str(csv_path), str(csv_path), CONSUMER_KEY, CONSUMER_SECRET,max_runtime_minutes=6
)

#%%