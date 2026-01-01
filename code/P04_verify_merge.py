"""
Enhanced Merge Verification with Data Analysis and Visualizations

Provides comprehensive statistics and insights about:
- URL coverage and gaps
- Source breakdown (songstats vs discogs)
- Missing songs analysis
- Actionable next steps
"""

import csv
from pathlib import Path
from collections import defaultdict

folder_path = Path(__file__).resolve().parents[1]
songstats_csv = folder_path / "data/spotify_playlists/main/liked_yt_songstats.csv"
discogs_csv = folder_path / "data/spotify_playlists/main/liked_yt_discogs.csv"
master_csv = folder_path / "data/spotify_playlists/main/liked_master.csv"

# ANSI color codes for better readability
class Colors:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    END = '\033[0m'

def print_header(text):
    """Print a styled header"""
    print(f"\n{Colors.BOLD}{Colors.CYAN}{'=' * 80}{Colors.END}")
    print(f"{Colors.BOLD}{Colors.CYAN}{text.center(80)}{Colors.END}")
    print(f"{Colors.BOLD}{Colors.CYAN}{'=' * 80}{Colors.END}\n")

def print_subheader(text):
    """Print a styled subheader"""
    print(f"\n{Colors.BOLD}{Colors.BLUE}▶ {text}{Colors.END}")
    print(f"{Colors.BLUE}{'─' * 78}{Colors.END}")

def print_success(text):
    """Print success message"""
    print(f"{Colors.GREEN}✓ {text}{Colors.END}")

def print_warning(text):
    """Print warning message"""
    print(f"{Colors.YELLOW}⚠ {text}{Colors.END}")

def print_error(text):
    """Print error message"""
    print(f"{Colors.RED}✗ {text}{Colors.END}")

def draw_bar_chart(label, value, max_value, width=50, color=Colors.GREEN):
    """Draw a simple ASCII bar chart"""
    filled = int((value / max_value) * width) if max_value > 0 else 0
    bar = '█' * filled + '░' * (width - filled)
    percentage = (value / max_value * 100) if max_value > 0 else 0
    print(f"  {label:30} {color}{bar}{Colors.END} {value:,} ({percentage:.1f}%)")

def load_csv(filepath):
    """Load CSV into dictionary keyed by track_uri"""
    data = {}
    with open(filepath, 'r', encoding='utf-8', newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            track_uri = row.get('track_uri', '').strip()
            if track_uri:
                data[track_uri] = row
    return data

# ============================================================================
# LOAD DATA
# ============================================================================
print_header("YouTube URL MERGE ANALYSIS & VERIFICATION")

print("Loading data files...")
songstats_data = load_csv(songstats_csv)
discogs_data = load_csv(discogs_csv)
master_data = load_csv(master_csv)

print_success(f"Loaded {len(songstats_data):,} tracks from songstats")
print_success(f"Loaded {len(discogs_data):,} tracks from discogs")
print_success(f"Loaded {len(master_data):,} tracks from master")

# ============================================================================
# VERIFICATION CHECKS
# ============================================================================
print_header("DATA INTEGRITY VERIFICATION")

# Check 1: All tracks present
missing_tracks = set(songstats_data.keys()) - set(master_data.keys())
if missing_tracks:
    print_error(f"{len(missing_tracks)} tracks from songstats missing in master!")
else:
    print_success("All tracks from songstats present in master")

# Check 2: Verify URL preservation
songstats_preserved = 0
songstats_total_urls = 0
for track_uri, song_row in songstats_data.items():
    song_url = song_row.get('yt_url', '').strip()
    song_status = song_row.get('status', '').strip()
    if song_url and song_status == 'done':
        songstats_total_urls += 1
        master_row = master_data.get(track_uri)
        if master_row and master_row.get('yt_url', '').strip() == song_url:
            songstats_preserved += 1

if songstats_preserved == songstats_total_urls:
    print_success(f"All {songstats_total_urls:,} songstats URLs preserved")
else:
    print_error(f"Only {songstats_preserved}/{songstats_total_urls} songstats URLs preserved")

# Check 3: Discogs fallbacks added
discogs_added = 0
discogs_available = 0
for track_uri, song_row in songstats_data.items():
    song_url = song_row.get('yt_url', '').strip()
    song_status = song_row.get('status', '').strip()
    if not (song_url and song_status == 'done'):
        discogs_row = discogs_data.get(track_uri)
        if discogs_row:
            discogs_url = discogs_row.get('yt_url', '').strip()
            discogs_status = discogs_row.get('status', '').strip()
            if discogs_url and discogs_status == 'done':
                discogs_available += 1
                master_row = master_data.get(track_uri)
                if master_row and master_row.get('yt_url', '').strip() == discogs_url:
                    discogs_added += 1

if discogs_added == discogs_available:
    print_success(f"All {discogs_available:,} available discogs URLs added as fallbacks")
else:
    print_warning(f"Only {discogs_added}/{discogs_available} discogs URLs added")

# ============================================================================
# COVERAGE ANALYSIS
# ============================================================================
print_header("URL COVERAGE ANALYSIS")

total_tracks = len(master_data)
urls_from_songstats = songstats_total_urls
urls_from_discogs = discogs_added
total_urls = urls_from_songstats + urls_from_discogs
missing_urls = total_tracks - total_urls

print_subheader("Overall Coverage")
draw_bar_chart("Songstats URLs", urls_from_songstats, total_tracks, color=Colors.GREEN)
draw_bar_chart("Discogs Fallback URLs", urls_from_discogs, total_tracks, color=Colors.CYAN)
draw_bar_chart("Missing URLs", missing_urls, total_tracks, color=Colors.RED)

print(f"\n  {Colors.BOLD}Total Coverage: {total_urls:,} / {total_tracks:,} tracks "
      f"({total_urls/total_tracks*100:.1f}%){Colors.END}")

# ============================================================================
# MISSING SONGS ANALYSIS
# ============================================================================
print_header("MISSING SONGS DETAILED ANALYSIS")

# Analyze why songs are missing
missing_songs = []
status_breakdown = defaultdict(int)
artist_missing_count = defaultdict(int)
album_missing_count = defaultdict(int)
decade_missing_count = defaultdict(int)

for track_uri, master_row in master_data.items():
    yt_url = master_row.get('yt_url', '').strip()
    status = master_row.get('status', '').strip()

    if not yt_url or status != 'done':
        missing_songs.append(master_row)
        status_breakdown[status if status else 'unknown'] += 1

        # Track by artist
        artist = master_row.get('artist_name(s)', 'Unknown')[:50]
        artist_missing_count[artist] += 1

        # Track by album
        album = master_row.get('album_name', 'Unknown')[:50]
        album_missing_count[album] += 1

        # Track by decade
        release_date = master_row.get('album_release_date', '')
        if len(release_date) >= 4:
            year = int(release_date[:4])
            decade = (year // 10) * 10
            decade_missing_count[f"{decade}s"] += 1
        else:
            decade_missing_count["Unknown"] += 1

print_subheader("Missing Songs by Status")
for status, count in sorted(status_breakdown.items(), key=lambda x: -x[1]):
    draw_bar_chart(status, count, missing_urls, color=Colors.YELLOW)

print_subheader("Top 10 Artists with Most Missing Songs")
top_artists = sorted(artist_missing_count.items(), key=lambda x: -x[1])[:10]
max_artist_count = top_artists[0][1] if top_artists else 1
for i, (artist, count) in enumerate(top_artists, 1):
    draw_bar_chart(f"{i}. {artist}", count, max_artist_count, width=40, color=Colors.YELLOW)

print_subheader("Top 10 Albums with Most Missing Songs")
top_albums = sorted(album_missing_count.items(), key=lambda x: -x[1])[:10]
max_album_count = top_albums[0][1] if top_albums else 1
for i, (album, count) in enumerate(top_albums, 1):
    draw_bar_chart(f"{i}. {album}", count, max_album_count, width=40, color=Colors.YELLOW)

print_subheader("Missing Songs by Decade")
for decade in sorted(decade_missing_count.keys()):
    if decade != "Unknown":
        count = decade_missing_count[decade]
        draw_bar_chart(decade, count, missing_urls, color=Colors.YELLOW)
if "Unknown" in decade_missing_count:
    count = decade_missing_count["Unknown"]
    draw_bar_chart("Unknown Year", count, missing_urls, color=Colors.RED)

# ============================================================================
# SAMPLE MISSING SONGS
# ============================================================================
print_subheader("Sample of Missing Songs (First 15)")
print()
for i, song in enumerate(missing_songs[:15], 1):
    track_name = song.get('track_name', 'Unknown')
    artist = song.get('artist_name(s)', 'Unknown')
    album = song.get('album_name', 'Unknown')
    status = song.get('status', 'unknown')

    status_color = Colors.YELLOW if status in ['no_yt', 'no_isrc'] else Colors.RED
    print(f"  {i:2}. {track_name[:40]:40} - {artist[:30]:30}")
    print(f"      Album: {album[:50]:50} [{status_color}{status}{Colors.END}]")

# ============================================================================
# RECOMMENDATIONS
# ============================================================================
print_header("ACTIONABLE RECOMMENDATIONS")

print_subheader("Next Steps to Improve Coverage")

if missing_urls > 0:
    no_isrc_count = status_breakdown.get('no_isrc', 0)
    no_yt_count = status_breakdown.get('no_yt', 0) + status_breakdown.get('', 0)

    if no_isrc_count > 0:
        print(f"\n  {Colors.BOLD}1. Fix Missing ISRC Codes{Colors.END}")
        print(f"     • {no_isrc_count:,} tracks are missing ISRC codes")
        print(f"     • These cannot be searched on Songstats without ISRC")
        print(f"     • Consider manual lookup or alternative data sources")

    if no_yt_count > 0:
        print(f"\n  {Colors.BOLD}2. Manual Search Needed{Colors.END}")
        print(f"     • {no_yt_count:,} tracks were not found on Songstats or Discogs")
        print(f"     • {no_yt_count:,} Will be searched with yt_dlp search feature. If these are too many, risk of overuse and yt throttling")        
        print(f"     • Consider direct YouTube search for these tracks")

    print(f"\n  {Colors.BOLD}3. Alternative Sources{Colors.END}")
    print(f"     • Try other metadata sources (MusicBrainz, Last.fm)")
    print(f"     • Direct YouTube API search by track name + artist")
    print(f"     • Check if tracks are available on YouTube at all")

    print(f"\n  {Colors.BOLD}4. Focus on High-Impact Areas{Colors.END}")
    if top_artists:
        top_artist_name, top_artist_count = top_artists[0]
        print(f"     • Start with '{top_artist_name}' ({top_artist_count} missing tracks)")
    if top_albums:
        top_album_name, top_album_count = top_albums[0]
        print(f"     • Or album '{top_album_name}' ({top_album_count} missing tracks)")
else:
    print_success("Perfect! All tracks have YouTube URLs!")

# ============================================================================
# FINAL SUMMARY
# ============================================================================
print_header("FINAL SUMMARY")

print(f"If urls left empty the search function of yt_dlp will be activated on next code P05_yt_download")
print(f"It is recommended to input as many valid urls as possible in order to make to workload smaller for the last step (download) to avoid throttling")
print(f" ")
print(f"{Colors.BOLD}Dataset Statistics:{Colors.END}")
print(f"  • Total tracks:               {total_tracks:>6,}")
print(f"  • URLs from Songstats:        {urls_from_songstats:>6,}  ({urls_from_songstats/total_tracks*100:>5.1f}%)")
print(f"  • URLs from Discogs:          {urls_from_discogs:>6,}  ({urls_from_discogs/total_tracks*100:>5.1f}%)")
print(f"  • {Colors.GREEN}Total URLs (Coverage):       {total_urls:>6,}  ({total_urls/total_tracks*100:>5.1f}%){Colors.END}")
print(f"  • {Colors.RED}Still Missing:               {missing_urls:>6,}  ({missing_urls/total_tracks*100:>5.1f}%){Colors.END}")

if songstats_preserved + discogs_added == total_urls:
    print(f"\n{Colors.BOLD}{Colors.GREEN}✅ VERIFICATION PASSED: All URLs accounted for!{Colors.END}\n")
else:
    print(f"\n{Colors.BOLD}{Colors.YELLOW}⚠️  WARNING: URL count mismatch detected{Colors.END}")
    print(f"   Expected: {total_urls:,}, Accounted: {songstats_preserved + discogs_added:,}\n")

print(f"{Colors.CYAN}{'=' * 80}{Colors.END}\n")
print(f"{Colors.BOLD}Output file: {Colors.END}{master_csv}")
print(f"{Colors.BOLD}Coverage:    {Colors.END}{Colors.GREEN if total_urls/total_tracks > 0.7 else Colors.YELLOW}"
      f"{total_urls/total_tracks*100:.1f}%{Colors.END}\n")
