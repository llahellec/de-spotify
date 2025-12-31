"""
Merge YouTube URLs from songstats and discogs CSVs into a master file.

PURPOSE:
This script combines YouTube links from two sources (Songstats and Discogs) to create
a master file with maximum URL coverage while maintaining data quality by prioritizing
the more reliable Songstats source.

INPUT FILES:
- liked_yt_songstats.csv: Output from songstats.py scraper
- liked_yt_discogs.csv: Output from discogs.py scraper

OUTPUT FILE:
- liked_master.csv: Merged file with best available URLs from both sources

MERGE LOGIC:
1. Load songstats CSV as the base (it's prioritized)
2. For each track:
   - If songstats has a valid URL (status='done'), keep it
   - If songstats has no URL or status='no_yt', check discogs
   - If discogs has a URL, use it and mark as 'discogs_fallback'
3. Save merged data with statistics

BENEFITS:
- Maximizes URL coverage by combining both sources
- Maintains data quality by prioritizing songstats
- Tracks the origin of each URL for transparency
- No external dependencies (uses only Python standard library)

USAGE:
    python code/merge_yt_urls.py

VERIFICATION:
After running, verify merge quality with:
    python code/verify_merge.py
"""

import csv
from pathlib import Path

# Paths
folder_path = Path(__file__).resolve().parents[1]
songstats_csv = folder_path / "data/spotify_playlists/main/liked_yt_songstats.csv"
discogs_csv = folder_path / "data/spotify_playlists/main/liked_yt_discogs.csv"
output_csv = folder_path / "data/spotify_playlists/main/liked_master.csv"

print("=" * 60)
print("YouTube URL Merger: Songstats + Discogs")
print("=" * 60)

# Load discogs data into a lookup dictionary
print(f"\nLoading {discogs_csv.name}...")
discogs_lookup = {}
discogs_count = 0

with open(discogs_csv, 'r', encoding='utf-8', newline='') as f:
    reader = csv.DictReader(f)
    for row in reader:
        track_uri = row.get('track_uri', '').strip()
        if track_uri:
            discogs_lookup[track_uri] = {
                'yt_url': row.get('yt_url', '').strip(),
                'status': row.get('status', '').strip()
            }
            discogs_count += 1

print(f"  Loaded {discogs_count} tracks from discogs")

# Process songstats data and merge
print(f"\nLoading and merging {songstats_csv.name}...")
songstats_count = 0
filled_from_discogs = 0
already_had_songstats = 0
total_urls_after_merge = 0

# Read and process in one pass
merged_rows = []
header = None

with open(songstats_csv, 'r', encoding='utf-8', newline='') as f:
    reader = csv.DictReader(f)
    header = reader.fieldnames

    for row in reader:
        songstats_count += 1
        track_uri = row.get('track_uri', '').strip()
        songstats_url = row.get('yt_url', '').strip()
        songstats_status = row.get('status', '').strip()

        # Check if songstats has a valid URL
        has_songstats_url = (
            songstats_url != '' and
            songstats_status == 'done'
        )

        if has_songstats_url:
            already_had_songstats += 1
            total_urls_after_merge += 1
        else:
            # Try to get URL from discogs
            if track_uri in discogs_lookup:
                discogs_data = discogs_lookup[track_uri]
                discogs_url = discogs_data['yt_url']
                discogs_status = discogs_data['status']

                # If discogs has a valid URL, use it
                if discogs_url != '' and discogs_status == 'done':
                    row['yt_url'] = discogs_url
                    row['status'] = 'done'
                    row['yt_url_origin'] = 'discogs_fallback'
                    filled_from_discogs += 1
                    total_urls_after_merge += 1

        merged_rows.append(row)

print(f"  Processed {songstats_count} tracks from songstats")
print(f"\nMerging completed!")

# Save merged file
print(f"\nSaving merged data to {output_csv.name}...")
with open(output_csv, 'w', encoding='utf-8', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=header)
    writer.writeheader()
    writer.writerows(merged_rows)

# Print summary statistics
print("\n" + "=" * 60)
print("MERGE SUMMARY")
print("=" * 60)
print(f"Total tracks:                    {songstats_count}")
print(f"URLs from songstats:             {already_had_songstats}")
print(f"URLs filled from discogs:        {filled_from_discogs}")
print(f"Total URLs after merge:          {total_urls_after_merge}")
print(f"Tracks still without URLs:       {songstats_count - total_urls_after_merge}")
print(f"\nCoverage: {total_urls_after_merge / songstats_count * 100:.1f}%")
print(f"\nOutput saved to: {output_csv}")
print("=" * 60)
