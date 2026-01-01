#%%

import os
import requests
import time
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Load credentials from .env file
consumer_key = os.getenv("DISCOGS_CONSUMER_KEY")
consumer_secret = os.getenv("DISCOGS_CONSUMER_SECRET")

# Example usage:
artist = "Pink Floyd"
album = "the dark side of the moon"

# Configuration
DISCOGS_SEARCH_LIMIT = 3      # number of results to inspect in each search
API_CALL_DELAY = 3          # pause before every API request (seconds)

def search_album_discogs(artist, title, key, secret):
    """Search by separate artist and release_title fields, restricted to masters."""
    time.sleep(API_CALL_DELAY)
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

def search_album_combined(artist, title, key, secret):
    """Search using the combined 'Artist - Title' string, restricted to masters."""
    time.sleep(API_CALL_DELAY)
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

def search_master_by_query(query, key, secret, per_page=1):
    """Perform a 'normal' query search, restricted to masters."""
    time.sleep(API_CALL_DELAY)
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

def search_album_release(artist, title, key, secret):
    """
    Search by separate artist and release_title fields with NO type filter.
    Useful when there is only a release and no master.
    """
    time.sleep(API_CALL_DELAY)
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

def fetch_master_videos(master_url, key, secret):
    """Fetch videos from a master; return [] on any error and print progress."""
    time.sleep(API_CALL_DELAY)
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

def fetch_release_videos(release_url, key, secret):
    """Fetch videos from a release; return [] on any error and print progress."""
    time.sleep(API_CALL_DELAY)
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

def try_result_list(results, key, secret):
    """Try each search result in order, printing progress, and return the first non-empty video list."""
    for i, result in enumerate(results, 1):
        master_url   = result.get("master_url")
        master_id    = result.get("master_id")
        resource_url = result.get("resource_url")
        print(f"  [Candidate {i}] title='{result.get('title')}', country='{result.get('country')}', year='{result.get('year')}'")
        videos = []
        if master_url:
            videos = fetch_master_videos(master_url, key, secret)
        if not videos and master_id:
            constructed = f"https://api.discogs.com/masters/{master_id}"
            print("    -> No master_url, using master_id")
            videos = fetch_master_videos(constructed, key, secret)
        if not videos and resource_url:
            print("    -> Falling back to release resource_url")
            videos = fetch_release_videos(resource_url, key, secret)
        if videos:
            return videos
    return []

def get_album_youtube_videos(artist, album, key, secret):
    """
    Run a series of searches from most specific to least specific, pausing before each API call:
    1) Fielded master search (artist + release_title)
    2) Combined master search ("Artist - Title")
    3) Release search without master filter
    4) General query master search (q parameter)
    """
    # 1. Fielded master search
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

    # 2. Combined master search
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

    # 3. Release search without master filter
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

    # 4. General query master search
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

videos = get_album_youtube_videos(artist, album, consumer_key, consumer_secret)
for title, url in videos:
    print(f"Matched video: {title} -> {url}")

#%%