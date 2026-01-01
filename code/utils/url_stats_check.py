"""
Checks presence of key columns on csvs, + some stats about how the scraping went (how many urls)
and their source
"""

import pandas as pd
from pathlib import Path

folder_path = Path(__file__).resolve().parents[2]
songstats_csv = folder_path / "data/spotify_playlists/main/liked_yt_songstats.csv"
discogs_csv = folder_path / "data/spotify_playlists/main/liked_yt_discogs.csv"

# Load both files
df_songstats = pd.read_csv(songstats_csv, encoding="UTF-8")
df_discogs = pd.read_csv(discogs_csv, encoding="UTF-8")

print("=== SONGSTATS DATA ===")
print(f"Total tracks: {len(df_songstats)}")
print(f"Tracks with yt_url: {df_songstats['yt_url'].notna().sum()}")
print(f"Non-empty yt_url: {(df_songstats['yt_url'].fillna('').astype(str).str.strip() != '').sum()}")
print(f"\nStatus counts:")
print(df_songstats['status'].value_counts())

print("\n=== DISCOGS DATA ===")
print(f"Total tracks: {len(df_discogs)}")
print(f"Tracks with yt_url: {df_discogs['yt_url'].notna().sum()}")
print(f"Non-empty yt_url: {(df_discogs['yt_url'].fillna('').astype(str).str.strip() != '').sum()}")
print(f"\nStatus counts:")
print(df_discogs['status'].value_counts())

print("\n=== COLUMNS ===")
print(f"Columns in both files match: {list(df_songstats.columns) == list(df_discogs.columns)}")
print(f"\nKey columns present:")
for col in ['track_uri', 'isrc', 'yt_url', 'status', 'yt_url_origin']:
    print(f"  - {col}: {col in df_songstats.columns}")
