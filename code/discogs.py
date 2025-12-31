#%%
"""
Discogs album-by-album YouTube link filler (resumable, single-CSV workflow)

Key points (mirrors your Songstats workflow):
- Loads the "latest" CSV: output if it exists else input (same pattern as Songstats). :contentReference[oaicite:0]{index=0}
- Resumes automatically using the SAME todo_mask idea: only rows with empty yt_url AND status not in ["done", "no_yt"].
- Processes album-by-album in the ORIGINAL CSV order (groupby(sort=False)) so it won’t jump to some other album.
- Saves progress VERY often:
  - after EACH album (strongest guarantee of resumability)
  - plus a checkpoint message every SAVE_EVERY_N albums
- Adds the same time-based progress bar style as your Songstats script. :contentReference[oaicite:1]{index=1}
- Enforces >=1.2s pause BEFORE EVERY Discogs API call.
"""
import re
import unicodedata

import os
import time
import random
import re
import pandas as pd
from pathlib import Path
from typing import List, Tuple, Dict, Optional

import requests
from dotenv import load_dotenv

###############################################################################
# Environment (optional: keep same behavior as Songstats)
###############################################################################
try:
    from IPython import get_ipython
    ipython = get_ipython()
    if ipython is not None:
        print("[Environment] Running in Interactive mode (IPython/Jupyter). Resetting workspace.")
        ipython.run_line_magic('reset', '-sf')
    else:
        print("[Environment] Running in Standard Python mode (non-interactive). No reset performed.")
except Exception:
    print("[Environment] Running in Standard Python mode (non-interactive). No reset performed.")

###############################################################################
# Load environment variables
###############################################################################
load_dotenv()

###############################################################################
# Discogs API configuration
###############################################################################
CONSUMER_KEY = os.getenv("DISCOGS_CONSUMER_KEY")
CONSUMER_SECRET = os.getenv("DISCOGS_CONSUMER_SECRET")

DISCOGS_SEARCH_LIMIT = 3
API_CALL_DELAY = 1.2  # must be applied before EVERY request

USER_AGENT = "Happyapi/1.0 (loic.lahellec@dauphine.eu)"


def _sleep_api():
    """Enforce minimum delay before every Discogs API call."""
    time.sleep(API_CALL_DELAY)


def _discogs_get(url: str, params: Dict) -> requests.Response:
    """Single place for GET + delay + headers."""
    _sleep_api()
    headers = {"User-Agent": USER_AGENT}
    resp = requests.get(url, params=params, headers=headers, timeout=30)
    return resp


def _pretty_kv(d: Dict, keys: List[str]) -> str:
    parts = []
    for k in keys:
        v = d.get(k)
        if v is None or v == "":
            continue
        parts.append(f"{k}={v!r}")
    return ", ".join(parts)


###############################################################################
# Discogs search functions
###############################################################################
def search_album_discogs_master_fielded(artist: str, title: str, key: str, secret: str) -> Dict:
    """Fielded search restricted to masters."""
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
    print(f"[Discogs] Fielded MASTER search: artist={artist!r}, release_title={title!r}")
    resp = _discogs_get(base_url, params=params)
    resp.raise_for_status()
    return resp.json()


def search_album_discogs_master_combined(artist: str, title: str, key: str, secret: str) -> Dict:
    """Combined title search restricted to masters."""
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
    print(f"[Discogs] Combined MASTER search: title={combined!r}")
    resp = _discogs_get(base_url, params=params)
    resp.raise_for_status()
    return resp.json()


def search_album_discogs_release_fielded(artist: str, title: str, key: str, secret: str) -> Dict:
    """
    Fielded search WITHOUT type filter (so it can return 'release' when no master exists).
    You told me this rescued some less-known albums.
    """
    base_url = "https://api.discogs.com/database/search"
    params = {
        "artist": artist,
        "release_title": title,
        "per_page": DISCOGS_SEARCH_LIMIT,
        "page": 1,
        "key": key,
        "secret": secret,
    }
    print(f"[Discogs] Fielded RELEASE search (no type): artist={artist!r}, release_title={title!r}")
    resp = _discogs_get(base_url, params=params)
    resp.raise_for_status()
    return resp.json()


def search_master_by_query(query: str, key: str, secret: str, per_page: int = DISCOGS_SEARCH_LIMIT) -> Dict:
    """Free-form query search restricted to masters."""
    base_url = "https://api.discogs.com/database/search"
    params = {
        "q": query,
        "type": "master",
        "per_page": per_page,
        "page": 1,
        "key": key,
        "secret": secret,
    }
    print(f"[Discogs] Query MASTER search: q={query!r}")
    resp = _discogs_get(base_url, params=params)
    resp.raise_for_status()
    return resp.json()

def clean_album_name_for_search(album_name: str) -> str:
    """
    Remove common edition/version/remaster/deluxe/etc. tags from album titles.
    - Strips bracket/parenthesis segments like "(2012 Remastered)", "[Deluxe Edition]"
    - Also strips trailing tag segments after separators like " - Deluxe Edition"
    - Normalizes accents/case for detection only.
    Returns a simplified album title suitable for Discogs search.

    Examples:
      "Album Name (2012 Remastered Num 4)" -> "Album Name"
      "Album Name (Deluxe Edition)"        -> "Album Name"
      "Album Name [Expanded]"              -> "Album Name"
      "Album Name - 2011 Remaster"         -> "Album Name"
      "Album Name (Live)"                  -> "Album Name"   (optional: tune keywords)
    """
    if not album_name or not isinstance(album_name, str):
        return album_name

    original = album_name

    def _norm(s: str) -> str:
        s = unicodedata.normalize("NFKD", s)
        s = "".join(ch for ch in s if not unicodedata.combining(ch))
        return s.lower()

    # Keywords that indicate "non-core title" tags
    # Add/remove words here depending on how aggressive you want it.
    TAG_KEYWORDS = {
        "remaster", "remastered", "remastering",
        "deluxe", "edition", "editions",
        "expanded", "expansion",
        "anniversary", "special", "collector", "collectors",
        "limited", "ultimate", "definitive",
        "version", "edit", "mix", "mono", "stereo",
        "bonus", "reissue", "rerelease",
        "extended", "digitally", "digital",
        "explicit", "clean",
        "super", "luxury",
        "original motion picture soundtrack", "soundtrack",
    }

    def _contains_tag_words(text: str) -> bool:
        n = _norm(text)
        # quick checks for common patterns
        if re.search(r"\b(19|20)\d{2}\b", n) and ("remaster" in n or "reissue" in n):
            return True
        # word-based check
        for w in TAG_KEYWORDS:
            if w in n:
                return True
        return False

    # 1) Remove bracketed chunks (...) or [...]
    # Only remove chunks that look like tags (contain keywords OR are mostly "taggy")
    bracket_pat = re.compile(r"(\([^)]*\)|\[[^\]]*\])")
    cleaned = album_name

    for m in list(bracket_pat.finditer(album_name)):
        chunk = m.group(0)
        inner = chunk[1:-1].strip()
        if not inner:
            continue

        # Remove if it contains tag words OR looks like a year/remaster-ish chunk
        if _contains_tag_words(inner):
            cleaned = cleaned.replace(chunk, " ")

    # 2) Remove trailing " - something" / " — something" / ": something" if "something" is taggy
    # Do this iteratively because you might have multiple suffixes.
    sep_pat = re.compile(r"^(.*?)(\s*[-–—:]\s*)(.+)$")
    while True:
        m = sep_pat.match(cleaned.strip())
        if not m:
            break
        left, sep, right = m.group(1).strip(), m.group(2), m.group(3).strip()
        if _contains_tag_words(right):
            cleaned = left
        else:
            break

    # 3) Final cleanup: collapse spaces, remove stray separators
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    cleaned = re.sub(r"\s*[-–—:]\s*$", "", cleaned).strip()

    # Safety fallback
    return cleaned if cleaned else original


###############################################################################
# Discogs fetch video functions
###############################################################################
def fetch_master_videos(master_url: str, key: str, secret: str) -> List[Tuple[str, str]]:
    """Fetch master JSON and return [(video_title, video_url), ...]. Return [] on any error."""
    params = {"key": key, "secret": secret}
    try:
        print(f"    -> Fetch MASTER: {master_url}")
        resp = _discogs_get(master_url, params=params)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"    ! MASTER fetch failed: {e}")
        return []
    vids = [
        (v.get("title", ""), v.get("uri"))
        for v in data.get("videos", [])
        if v.get("title") and v.get("uri")
    ]
    print(f"    -> MASTER videos found: {len(vids)}")
    return vids


def fetch_release_videos(resource_url: str, key: str, secret: str) -> List[Tuple[str, str]]:
    """Fetch release JSON and return [(video_title, video_url), ...]. Return [] on any error."""
    params = {"key": key, "secret": secret}
    try:
        print(f"    -> Fetch RELEASE: {resource_url}")
        resp = _discogs_get(resource_url, params=params)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"    ! RELEASE fetch failed: {e}")
        return []
    vids = [
        (v.get("title", ""), v.get("uri"))
        for v in data.get("videos", [])
        if v.get("title") and v.get("uri")
    ]
    print(f"    -> RELEASE videos found: {len(vids)}")
    return vids


def _try_result_list(results: List[Dict], key: str, secret: str) -> List[Tuple[str, str]]:
    """
    Iterate candidates in order. Try:
      1) master_url
      2) constructed master URL from master_id
      3) resource_url as release
    Return the first non-empty video list.
    """
    for j, r in enumerate(results, start=1):
        print(f"  [Candidate {j}/{len(results)}] {_pretty_kv(r, ['type','title','country','year','id','master_id'])}")
        master_url = r.get("master_url")
        master_id = r.get("master_id")
        resource_url = r.get("resource_url")

        vids: List[Tuple[str, str]] = []

        if master_url:
            vids = fetch_master_videos(master_url, key, secret)

        if not vids and master_id:
            constructed = f"https://api.discogs.com/masters/{master_id}"
            print("    -> No vids yet, trying constructed master URL from master_id")
            vids = fetch_master_videos(constructed, key, secret)

        if not vids and resource_url:
            print("    -> No vids yet, trying resource_url as RELEASE")
            vids = fetch_release_videos(resource_url, key, secret)

        if vids:
            print("  -> Using this candidate (videos found).")
            return vids

    return []


def get_album_youtube_videos(artist: str, album: str, key: str, secret: str) -> List[Tuple[str, str]]:
    """
    Priority (best-of-both-worlds):
      1) Fielded MASTER (most “consistent” when correct)
      2) Combined MASTER (helps when title formatting differs)
      3) Fielded RELEASE (no type filter)  <-- you asked to put this before query search
      4) Query MASTER (closest to website-like search)
    """
    # 1) Fielded master
    try:
        data = search_album_discogs_master_fielded(artist, album, key, secret)
        results = data.get("results", [])
        print(f"[Discogs] Fielded MASTER results: {len(results)}")
        vids = _try_result_list(results, key, secret)
        if vids:
            return vids
    except Exception as e:
        print(f"[Discogs] Fielded MASTER error: {e}")

    # 2) Combined master
    try:
        data = search_album_discogs_master_combined(artist, album, key, secret)
        results = data.get("results", [])
        print(f"[Discogs] Combined MASTER results: {len(results)}")
        vids = _try_result_list(results, key, secret)
        if vids:
            return vids
    except Exception as e:
        print(f"[Discogs] Combined MASTER error: {e}")

    # 3) Fielded release (no type filter)
    try:
        data = search_album_discogs_release_fielded(artist, album, key, secret)
        results = data.get("results", [])
        print(f"[Discogs] Fielded RELEASE results: {len(results)}")
        vids = _try_result_list(results, key, secret)
        if vids:
            return vids
    except Exception as e:
        print(f"[Discogs] Fielded RELEASE error: {e}")

    # 4) Query master
    try:
        q = f"{artist} {album}"
        data = search_master_by_query(q, key, secret, per_page=DISCOGS_SEARCH_LIMIT)
        results = data.get("results", [])
        print(f"[Discogs] Query MASTER results: {len(results)}")
        vids = _try_result_list(results, key, secret)
        if vids:
            return vids
    except Exception as e:
        print(f"[Discogs] Query MASTER error: {e}")

    print("[Discogs] No videos found after all strategies.")
    return []


###############################################################################
# Matching logic (more algorithmic than pure substring)
###############################################################################
_STOPWORDS = {
    "remaster", "remastered", "mix", "edit", "version", "mono", "stereo",
    "live", "demo", "radio", "explicit", "clean", "instrumental",
    "bonus", "deluxe", "anniversary", "extended", "feat", "featuring",
    "official", "video", "audio", "lyrics", "lyric", "hd", "4k"
}


def normalise_string(s: str) -> str:
    s = (s or "").lower()
    s = re.sub(r"[\-_]", " ", s)
    s = re.sub(r"[’']", "", s)
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _tokenize(s: str) -> List[str]:
    s = normalise_string(s)
    tokens = [t for t in s.split() if t and t not in _STOPWORDS]
    return tokens


def _token_containment_score(track: str, video_title: str) -> float:
    """
    Score in [0,1]. We require the majority of "meaningful" track tokens to appear in video title tokens.
    This fixes cases like: "Song Name - Remastered" vs "Song Name".
    """
    t_tokens = _tokenize(track)
    v_tokens = set(_tokenize(video_title))
    if not t_tokens:
        return 0.0
    hit = sum(1 for t in t_tokens if t in v_tokens)
    return hit / max(1, len(t_tokens))


def match_videos_to_tracks(
    videos: List[Tuple[str, str]],
    track_names: List[str],
    min_token_score: float = 0.66
) -> Dict[int, Tuple[str, str]]:
    """
    Returns {track_index_in_album: (video_title, video_url)}.
    Priority:
      - direct normalized substring match (fast)
      - token containment score >= min_token_score (more robust)
    """
    matched: Dict[int, Tuple[str, str]] = {}
    norm_tracks = [normalise_string(t) for t in track_names]

    for vid_title, vid_url in videos:
        nt = normalise_string(vid_title)

        for idx, tr in enumerate(track_names):
            if idx in matched:
                continue

            ntr = norm_tracks[idx]

            # Fast: substring either way
            if ntr and (ntr in nt or nt in ntr):
                matched[idx] = (vid_title, vid_url)
                break

            # Robust: token containment
            score = _token_containment_score(tr, vid_title)
            if score >= min_token_score:
                matched[idx] = (vid_title, vid_url)
                break

    return matched


###############################################################################
# Album-by-album update (resumable)
###############################################################################
def update_yt_links_with_discogs(
    input_csv: str,
    output_csv: str,
    key: str,
    secret: str,
    max_runtime_minutes: int = 60,
    save_every_n_albums: int = 10,
) -> None:
    df = pd.read_csv(input_csv, encoding="UTF-8")

    # Ensure columns
    for col in ["yt_url", "status", "yt_url_origin"]:
        if col not in df.columns:
            df[col] = ""

    df["yt_url"] = df["yt_url"].fillna("").astype(str)
    df["status"] = df["status"].fillna("").astype(str)
    df["yt_url_origin"] = df["yt_url_origin"].fillna("").astype(str)

    # Required cols (your current schema)
    album_cols = ["album_name", "album_artist_name(s)"]
    if not all(c in df.columns for c in album_cols):
        raise ValueError("CSV must contain 'album_name' and 'album_artist_name(s)' columns.")
    if "track_name" not in df.columns:
        raise ValueError("CSV must contain 'track_name' column.")

    # ---- RESUME LOGIC (same idea as Songstats) ----
    # Only do rows that still need something:
    todo_mask = (df["yt_url"].str.strip() == "") & (~df["status"].isin(["done", "no_yt"]))
    todo_idx = df.index[todo_mask].tolist()
    total_todo_rows = len(todo_idx)

    if todo_idx:
        first_idx = todo_idx[0]
        print(f"Loaded: {input_csv}")
        print(f"Total rows: {len(df)}")
        print(f"Todo rows:  {total_todo_rows}")
        print(f"Resuming from row index {first_idx} with status '{df.at[first_idx, 'status']}'.\n")
    else:
        print("Nothing to do: no rows match todo_mask (missing yt_url and not done/no_yt).")
        # Still save to ensure columns exist if it was the first run
        df.to_csv(output_csv, index=False)
        print(f"Data saved to: {output_csv}")
        return

    # Group ONLY todo rows, keep order of first appearance (sort=False is crucial)
    todo_df = df.loc[todo_idx, album_cols + ["track_name"]]
    groups = todo_df.groupby(album_cols, sort=False)

    total_albums = len(groups)
    print(f"Albums to process (from todo rows): {total_albums}\n")

    processed_albums = 0
    updated_tracks = 0

    start_time = time.time()
    max_runtime_seconds = max_runtime_minutes * 60

    for (album_name, album_artist), g in groups:
        elapsed = time.time() - start_time
        if elapsed > max_runtime_seconds:
            print(f"Maximum runtime of {max_runtime_minutes} minutes reached. Stopping gracefully.")
            break

        processed_albums += 1
        album_row_idxs = g.index.tolist()

        # If user already manually checked / verified "no yt" and set statuses, this album won't appear here.
        # Still, double-check:
        if not any(todo_mask.loc[album_row_idxs]):
            print(f"[{processed_albums}/{total_albums}] Album already resolved by status/yt_url, skipping.")
            continue

        print(f"\n[{processed_albums}/{total_albums}] Album: {album_name!r} | Artist: {album_artist!r}")
        print(f"  -> Todo tracks in this album: {len(album_row_idxs)}")

        track_names = df.loc[album_row_idxs, "track_name"].tolist()

        # Discogs lookup once per album
        album_name_search = clean_album_name_for_search(album_name)
        if album_name_search != album_name:
            print(f"  -> Album name cleaned for search: {album_name!r} -> {album_name_search!r}")

        videos = get_album_youtube_videos(album_artist, album_name_search, key, secret)


        if not videos:
            print("  -> No videos found. Marking remaining todo tracks as 'no_yt'.")
            for ridx in album_row_idxs:
                if df.at[ridx, "yt_url"].strip() == "" and df.at[ridx, "status"] not in ("done",):
                    df.at[ridx, "status"] = "no_yt"

            # Save AFTER each album so restart always resumes correctly
            df.to_csv(output_csv, index=False)
            print("  -> Saved (album checkpoint).")
        else:
            print(f"  -> Videos returned: {len(videos)}. Matching to tracks…")
            match_map = match_videos_to_tracks(videos, track_names, min_token_score=0.66)
            print(f"  -> Matched {len(match_map)}/{len(track_names)} track(s).")

            # Apply matches
            for local_idx, (vtitle, vurl) in match_map.items():
                ridx = album_row_idxs[local_idx]
                if df.at[ridx, "yt_url"].strip() == "":
                    df.at[ridx, "yt_url"] = vurl
                    df.at[ridx, "yt_url_origin"] = "discogs"
                    df.at[ridx, "status"] = "done"
                    updated_tracks += 1
                    print(f"    ✓ {track_names[local_idx]!r}  ->  {vurl}")

            # Mark remaining todo tracks in this album as no_yt
            for ridx in album_row_idxs:
                yt_val = df.at[ridx, "yt_url"]
                yt_val = yt_val if isinstance(yt_val, str) else str(yt_val)
                if yt_val.strip() == "":
                    if df.at[ridx, "status"] not in ("done",):
                        df.at[ridx, "status"] = "no_yt"

            # Save AFTER each album (strong resumability)
            df.to_csv(output_csv, index=False)
            print("  -> Saved (album checkpoint).")

        # Periodic “big” checkpoint message (even though we already save per album)
        if processed_albums % save_every_n_albums == 0:
            df.to_csv(output_csv, index=False)
            print(f"Checkpoint: saved after processing {processed_albums} albums.")

        # Time progress bar (same style as Songstats)
        progress = min(1.0, (time.time() - start_time) / max_runtime_seconds)
        bar_len = 20
        filled = int(bar_len * progress)
        bar = "█" * filled + "-" * (bar_len - filled)
        print(f"Time Progress: |{bar}| {progress*100:.1f}%")

        # (Optional) small jitter to be a good citizen beyond the strict 1.2s Discogs delay
        time.sleep(0.2 + random.uniform(0, 0.4))

    elapsed_minutes = (time.time() - start_time) / 60
    print(
        f"\nFinished run. Albums processed: {processed_albums}/{total_albums}, "
        f"Tracks updated: {updated_tracks}, Elapsed: {elapsed_minutes:.2f} minutes"
    )
    df.to_csv(output_csv, index=False)
    print(f"Data saved to: {output_csv}")


###############################################################################
# Single-CSV workflow init (like your Songstats script)
###############################################################################
MAX_RUNTIME_MINUTES = 120
SAVE_EVERY_N_ALBUMS = 20

folder_path = Path(__file__).resolve().parents[1]
input_csv_path = folder_path / "data/spotify_playlists/main/liked_yt_discogs.csv"
output_csv_path = input_csv_path  # progress on a single CSV

# Load “latest”: same pattern as Songstats (output if exists else input)
csv_to_load = output_csv_path if os.path.exists(output_csv_path) else input_csv_path

print(f"\nStarting Discogs scraping step…\nUsing CSV: {csv_to_load}\n")

update_yt_links_with_discogs(
    input_csv=str(csv_to_load),
    output_csv=str(output_csv_path),
    key=CONSUMER_KEY,
    secret=CONSUMER_SECRET,
    max_runtime_minutes=MAX_RUNTIME_MINUTES,
    save_every_n_albums=SAVE_EVERY_N_ALBUMS,
)

#%%
