# YT-DLP MUSIC DOWNLOADER
# =======================
# Ultimate script to download music library from YouTube
# Based on liked_master.csv with metadata from Spotify/Songstats/Discogs
#
# Features:
# - FULLY RESUMABLE: Saves after EACH download, restart anytime
# - SPOTIFY METADATA: Embeds all metadata from CSV (ID3v2.3 - most portable)
# - ALBUM ART: Downloads and embeds cover art from Spotify
# - Duration validation to ensure correct song matches
# - YouTube search fallback when no URL available
# - Organized output: Artist/Album/Song.mp3
# - Rate limiting and anti-detection measures
# - Best quality MP3

# ---------------------------
#  1. Import dependencies
# ---------------------------

try:
    from IPython import get_ipython
    ipython = get_ipython()
    if ipython is not None:
        print("[Environment] Running in Interactive mode (IPython/Jupyter). Resetting workspace.")
        ipython.run_line_magic('reset', '-sf')
    else:
        print("[Environment] Running in Standard Python mode.")
except (ImportError, AttributeError):
    print("[Environment] Running in Standard Python mode.")

import os
import re
import sys
import time
import random
import shutil
import tempfile
import pandas as pd
import requests
from pathlib import Path
from datetime import datetime
from io import BytesIO

try:
    import yt_dlp
except ImportError:
    print("ERROR: yt-dlp not installed!")
    print("Install with: pip install yt-dlp")
    exit(1)

try:
    from mutagen.id3 import ID3, TIT2, TPE1, TPE2, TALB, TRCK, TPOS, TCON, TYER, TDRC, TSRC, TPUB, TCOP, APIC, ID3NoHeaderError
    from mutagen.mp3 import MP3
    MUTAGEN_AVAILABLE = True
except ImportError:
    print("WARNING: mutagen not installed - metadata embedding disabled")
    print("Install with: pip install mutagen")
    MUTAGEN_AVAILABLE = False

# Add ffmpeg to PATH if not already available (WinGet installation location)
import subprocess
try:
    subprocess.run(['ffmpeg', '-version'], capture_output=True, check=True)
except (FileNotFoundError, subprocess.CalledProcessError):
    ffmpeg_path = os.path.expanduser(
        "~/AppData/Local/Microsoft/WinGet/Packages/Gyan.FFmpeg_Microsoft.Winget.Source_8wekyb3d8bbwe/ffmpeg-8.0.1-full_build/bin"
    )
    if os.path.exists(ffmpeg_path):
        os.environ["PATH"] = ffmpeg_path + os.pathsep + os.environ.get("PATH", "")
        print(f"[Environment] Added ffmpeg to PATH from: {ffmpeg_path}")

# Check for deno (required by yt-dlp for JavaScript runtime)
DENO_AVAILABLE = False
try:
    subprocess.run(['deno', '--version'], capture_output=True, check=True)
    DENO_AVAILABLE = True
    print("[Environment] Deno runtime available")
except (FileNotFoundError, subprocess.CalledProcessError):
    print("[Environment] WARNING: Deno not found - you may see JavaScript runtime warnings")
    print("             Install with: curl -fsSL https://deno.land/install.sh | sh")
    print("             Or on Windows: irm https://deno.land/install.ps1 | iex")

# ---------------------------
#  2. Configuration
# ---------------------------

# Runtime settings
MAX_RUNTIME_MINUTES = 800  # How long to run before stopping
SAVE_EVERY_N = 1  # Save CSV after every N downloads (1 = after each, fully resumable)
MAX_DOWNLOADS_THIS_RUN = None  # Set to integer to limit, None for unlimited

# Download settings
DURATION_TOLERANCE_PERCENT = 15  # Allow 15% duration difference for validation
DURATION_TOLERANCE_SECONDS = 30  # Or 30 seconds, whichever is greater
SLEEP_BETWEEN_DOWNLOADS = (3, 6)  # Random sleep range in seconds
SLEEP_BETWEEN_SEARCHES = (2, 4)  # Sleep when using ytsearch

# Anti-detection / Rate limit protection
MAX_CONSECUTIVE_FAILURES = 5  # Pause after this many failures in a row
RATE_LIMIT_PAUSE_MINUTES = 15  # How long to pause when rate limited
LONG_PAUSE_EVERY_N = 25  # Take a long break every N downloads
LONG_PAUSE_RANGE = (60, 180)  # Long pause duration range (1-3 minutes)

# Audio quality settings
AUDIO_FORMAT = "mp3"
AUDIO_QUALITY = "0"  # 0 = best quality (320kbps for mp3 if available)

# Metadata settings
EMBED_METADATA = True  # Enable/disable metadata embedding
EMBED_ALBUM_ART = True  # Download and embed album art from Spotify URL

# Paths
folder_path = Path(__file__).resolve().parents[1]
input_csv = folder_path / "data/spotify_playlists/main/liked_master.csv"
output_csv = input_csv  # Single CSV workflow - update in place (same as URL scrapers)
download_dir = folder_path / "downloads"

# ---------------------------
#  2b. Authentication & Advanced yt-dlp Options
# ---------------------------
# These options help bypass bot detection, access age-restricted/private content,
# and improve download reliability. See: https://github.com/yt-dlp/yt-dlp

# COOKIE AUTHENTICATION (Critical for avoiding "Sign in to confirm you're not a bot")
# Option 1: Auto-extract from browser (RECOMMENDED - easiest)
#   Set to browser name: "chrome", "firefox", "edge", "brave", "opera", "safari", "chromium", "vivaldi"
#   Set to None to disable browser cookie extraction
COOKIES_FROM_BROWSER = "firefox"  # Auto-extract cookies from this browser

# Option 2: Use exported cookies file (alternative to browser extraction)
#   Export cookies using browser extension or: yt-dlp --cookies-from-browser chrome --cookies cookies.txt
#   IMPORTANT: Export from a PRIVATE/INCOGNITO window, then close it immediately!
#   See: https://github.com/yt-dlp/yt-dlp/wiki/Extractors#exporting-youtube-cookies
COOKIES_FILE = folder_path / "cookies.txt"  # Path to cookies.txt file (used if browser extraction fails/disabled)

# YOUTUBE EXTRACTOR OPTIONS
# Player clients to try (in order). Multiple clients = better fallback.
# Available: "web", "web_safari", "web_embedded", "web_music", "web_creator",
#            "android", "android_music", "android_creator", "android_vr",
#            "ios", "ios_music", "ios_creator", "mweb", "tv", "tv_embedded", "mediaconnect"
# "web" works best with cookies. "android" sometimes bypasses restrictions.
YOUTUBE_PLAYER_CLIENTS = "web,android"  # Comma-separated list of clients to try

# PO Token (Proof of Origin) - Required for some videos to avoid 403 errors
# Format: "CLIENT.REQUEST_TYPE+TOKEN" e.g., "web.gvs+abc123..."
# Get tokens using: https://github.com/yt-dlp/yt-dlp/wiki/PO-Token-Guide
# Or install plugin: pip install bgutil-ytdlp-pot-provider (auto-generates tokens)
YOUTUBE_PO_TOKEN = None  # Set to token string if you have one, or None to skip

# Visitor data (alternative to cookies for some requests)
YOUTUBE_VISITOR_DATA = None  # Set to visitor_data string if needed, or None

# NETWORK OPTIONS
SOCKET_TIMEOUT = 30  # Connection timeout in seconds (default: None = no timeout)
FORCE_IPV4 = True  # Force IPv4 connections (more stable on some networks)
SOURCE_ADDRESS = None  # Bind to specific IP address (None = auto)

# RETRY OPTIONS
HTTP_RETRIES = 10  # Number of retries for HTTP errors (default: 10)
FRAGMENT_RETRIES = 10  # Number of retries for fragment downloads (default: 10)
EXTRACTOR_RETRIES = 5  # Number of retries for extractor errors (default: 3)
FILE_ACCESS_RETRIES = 5  # Number of retries for file access errors (default: 3)
RETRY_SLEEP_FUNCTIONS = "http:exp=1:30,fragment:exp=1:10"  # Exponential backoff: start 1s, max 30s/10s

# WORKAROUND OPTIONS
# Custom user agent - helps avoid detection. Set to None to use yt-dlp default.
# Get yours from: https://www.whatismybrowser.com/detect/what-is-my-user-agent/
USER_AGENT = None  # e.g., "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36..."

# Sleep between extraction requests (helps avoid rate limiting during search/info fetch)
SLEEP_REQUESTS = 1.5  # Seconds to sleep between requests to same host (default: 0)

# GEO-BYPASS OPTIONS
GEO_BYPASS = True  # Bypass geographic restrictions using fake X-Forwarded-For header
GEO_BYPASS_COUNTRY = None  # Two-letter country code (e.g., "US") or None for auto

# AGE RESTRICTION
AGE_LIMIT = None  # Max age rating to download (None = no limit, requires cookies for 18+)

# CONCURRENT DOWNLOADS (for fragmented formats)
CONCURRENT_FRAGMENTS = 4  # Number of fragments to download simultaneously (default: 1)

# RATE LIMITING
# THROTTLED_RATE - DISABLED (was causing type comparison errors)
# THROTTLED_RATE = 100000  # Bytes per second (100KB/s)

# IMPERSONATION (requires curl_cffi: pip install curl_cffi)
# Impersonate browser TLS fingerprint to bypass advanced bot detection
# Options: "chrome", "edge", "safari", or None to disable
IMPERSONATE_BROWSER = None  # Set to browser name or None

# ---------------------------
#  3. Helper Functions
# ---------------------------

class Tee:
    """Write output to multiple streams (e.g., console + file). Fail-safe."""
    def __init__(self, console, log_file=None):
        self.console = console
        self.log_file = log_file

    def write(self, text):
        # Always write to console first
        try:
            self.console.write(text)
            self.console.flush()
        except:
            pass
        # Try log file, but don't fail if it errors
        if self.log_file:
            try:
                self.log_file.write(text)
                self.log_file.flush()
            except:
                pass  # Silently ignore log write errors

    def flush(self):
        try:
            self.console.flush()
        except:
            pass
        if self.log_file:
            try:
                self.log_file.flush()
            except:
                pass


def atomic_save_csv(df: pd.DataFrame, filepath: Path) -> bool:
    """
    Save DataFrame to CSV atomically to prevent data corruption.
    Writes to temp file first, then moves to final location.
    """
    try:
        # Create temp file in same directory (for atomic move)
        temp_path = filepath.with_suffix('.csv.tmp')
        df.to_csv(temp_path, index=False)
        # Atomic move (on same filesystem)
        shutil.move(str(temp_path), str(filepath))
        return True
    except Exception as e:
        print(f"       [WARNING] Failed to save CSV: {e}")
        # Try to clean up temp file
        try:
            if temp_path.exists():
                temp_path.unlink()
        except:
            pass
        return False


def sanitize_filename(name: str) -> str:
    """Remove or replace characters that are invalid in filenames."""
    if not name:
        return "Unknown"
    # Replace problematic characters
    name = re.sub(r'[<>:"/\\|?*]', '_', name)
    name = re.sub(r'\s+', ' ', name).strip()
    name = name.strip('.')  # Remove trailing dots
    # Limit length
    if len(name) > 100:
        name = name[:100].strip()
    return name or "Unknown"


def get_primary_artist(artist_names: str) -> str:
    """Extract the first/primary artist from a comma-separated list."""
    if not artist_names or pd.isna(artist_names):
        return "Unknown Artist"
    # Artists might be separated by ", " in the CSV
    artists = str(artist_names).split(',')
    return sanitize_filename(artists[0].strip())


def clean_artist_string(artist_names: str) -> str:
    """Clean artist string for metadata (keep all artists, just clean up)."""
    if not artist_names or pd.isna(artist_names):
        return "Unknown Artist"
    # Remove 'spotify:artist:' prefixes if present
    cleaned = re.sub(r'spotify:artist:\w+,?\s*', '', str(artist_names))
    # Clean up multiple spaces
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    # Replace ", " with " / " for better display
    cleaned = cleaned.replace(', ', ' / ')
    return cleaned if cleaned else "Unknown Artist"


def format_duration(ms: float) -> str:
    """Convert milliseconds to MM:SS format."""
    if pd.isna(ms) or ms <= 0:
        return "??:??"
    total_seconds = int(ms / 1000)
    minutes = total_seconds // 60
    seconds = total_seconds % 60
    return f"{minutes}:{seconds:02d}"


def format_seconds(sec: float) -> str:
    """Convert seconds to MM:SS format."""
    if not sec or sec <= 0:
        return "??:??"
    minutes = int(sec) // 60
    seconds = int(sec) % 60
    return f"{minutes}:{seconds:02d}"


def duration_matches(expected_ms: float, actual_seconds: float) -> bool:
    """Check if actual duration is within tolerance of expected duration."""
    if pd.isna(expected_ms) or expected_ms <= 0:
        return True  # Can't validate, assume OK

    expected_seconds = expected_ms / 1000

    # Calculate tolerance
    percent_tolerance = expected_seconds * (DURATION_TOLERANCE_PERCENT / 100)
    tolerance = max(percent_tolerance, DURATION_TOLERANCE_SECONDS)

    return abs(actual_seconds - expected_seconds) <= tolerance


def build_search_query(track_name: str, artist_names: str) -> str:
    """Build an optimized search query for yt-dlp ytsearch."""
    artist = get_primary_artist(artist_names)
    track = sanitize_filename(track_name) if track_name else "Unknown Track"

    # Clean up track name - remove common suffixes that might confuse search
    track_clean = re.sub(r'\s*[-–]\s*(Remaster(ed)?|Remix|Live|Radio Edit|Single Version).*$', '', track, flags=re.I)
    track_clean = re.sub(r'\s*\([^)]*Remaster[^)]*\)', '', track_clean, flags=re.I)

    return f"{artist} {track_clean}"


def get_output_template(artist_names: str, album_name: str, track_name: str) -> str:
    """Generate the output path template for a track."""
    artist = get_primary_artist(artist_names)
    album = sanitize_filename(album_name) if album_name and not pd.isna(album_name) else "Unknown Album"
    track = sanitize_filename(track_name) if track_name else "Unknown Track"

    # Build path: downloads/Artist/Album/Track.mp3
    output_path = download_dir / artist / album / f"{track}.%(ext)s"
    return str(output_path)


def print_separator(char="-", length=60):
    """Print a separator line."""
    print(char * length)


def print_stats(df, processed, downloaded, failed, searched, start_time):
    """Print current session statistics."""
    elapsed = time.time() - start_time
    elapsed_min = elapsed / 60

    total = len(df)
    done_count = (df["downloaded"] == "yes").sum()
    done_pct = (done_count / total * 100) if total > 0 else 0

    print_separator("=")
    print("CURRENT SESSION STATS")
    print_separator("-")
    print(f"  Elapsed time:    {elapsed_min:.1f} minutes")
    print(f"  Processed:       {processed}")
    print(f"  Downloaded:      {downloaded}")
    print(f"  Failed:          {failed}")
    print(f"  Used search:     {searched}")
    print_separator("-")
    print(f"  TOTAL LIBRARY:   {total} tracks")
    print(f"  DOWNLOADED:      {done_count} ({done_pct:.1f}%)")
    print(f"  REMAINING:       {total - done_count}")
    print_separator("=")


# ---------------------------
#  4. Metadata Embedding (ID3v2.3)
# ---------------------------

def download_album_art(url: str) -> bytes | None:
    """Download album art from URL and return as bytes."""
    if not url or pd.isna(url):
        return None
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            return response.content
    except Exception as e:
        print(f"       Warning: Could not download album art: {e}")
    return None


def embed_metadata(file_path: str, metadata: dict) -> bool:
    """
    Embed ID3v2.3 metadata into MP3 file.
    Uses most portable format for maximum compatibility.

    Supported metadata keys:
    - title, artist, album_artist, album
    - year, track_number, disc_number
    - genre, isrc, label, copyright
    - album_art_url (will download and embed)
    """
    if not MUTAGEN_AVAILABLE:
        return False

    if not os.path.exists(file_path):
        print(f"       Warning: File not found for metadata: {file_path}")
        return False

    try:
        # Try to load existing ID3 tags, or create new ones
        try:
            audio = ID3(file_path)
        except ID3NoHeaderError:
            audio = ID3()

        # Clear existing tags to start fresh with our metadata
        audio.delete()
        audio = ID3()

        # Title (TIT2)
        if metadata.get('title'):
            audio.add(TIT2(encoding=3, text=str(metadata['title'])))

        # Artist (TPE1) - can be multiple artists
        if metadata.get('artist'):
            audio.add(TPE1(encoding=3, text=str(metadata['artist'])))

        # Album Artist (TPE2)
        if metadata.get('album_artist'):
            audio.add(TPE2(encoding=3, text=str(metadata['album_artist'])))

        # Album (TALB)
        if metadata.get('album'):
            audio.add(TALB(encoding=3, text=str(metadata['album'])))

        # Year (TYER for ID3v2.3 compatibility, TDRC for v2.4)
        if metadata.get('year'):
            year_str = str(metadata['year'])[:4]  # Just the year
            audio.add(TYER(encoding=3, text=year_str))
            audio.add(TDRC(encoding=3, text=year_str))

        # Track Number (TRCK) - format: "track/total" or just "track"
        if metadata.get('track_number'):
            track_str = str(int(float(metadata['track_number'])))
            audio.add(TRCK(encoding=3, text=track_str))

        # Disc Number (TPOS)
        if metadata.get('disc_number'):
            disc_str = str(int(float(metadata['disc_number'])))
            audio.add(TPOS(encoding=3, text=disc_str))

        # Genre (TCON)
        if metadata.get('genre'):
            # Take first genre if multiple
            genre = str(metadata['genre']).split(',')[0].strip()
            if genre:
                audio.add(TCON(encoding=3, text=genre))

        # ISRC (TSRC)
        if metadata.get('isrc'):
            audio.add(TSRC(encoding=3, text=str(metadata['isrc'])))

        # Publisher/Label (TPUB)
        if metadata.get('label'):
            audio.add(TPUB(encoding=3, text=str(metadata['label'])))

        # Copyright (TCOP)
        if metadata.get('copyright'):
            # Truncate if too long
            copyright_text = str(metadata['copyright'])[:200]
            audio.add(TCOP(encoding=3, text=copyright_text))

        # Album Art (APIC) - download and embed
        if EMBED_ALBUM_ART and metadata.get('album_art_url'):
            art_data = download_album_art(metadata['album_art_url'])
            if art_data:
                # Determine image type
                mime_type = 'image/jpeg'
                if metadata['album_art_url'].lower().endswith('.png'):
                    mime_type = 'image/png'

                audio.add(APIC(
                    encoding=3,
                    mime=mime_type,
                    type=3,  # 3 = front cover
                    desc='Cover',
                    data=art_data
                ))

        # Save with ID3v2.3 for maximum compatibility
        audio.save(file_path, v2_version=3)
        return True

    except Exception as e:
        print(f"       Warning: Could not embed metadata: {e}")
        return False


def extract_metadata_from_row(row: pd.Series) -> dict:
    """Extract metadata dictionary from CSV row."""

    def safe_get(key, default=""):
        val = row.get(key, default)
        if pd.isna(val):
            return default
        return val

    # Extract year from album_release_date (format: YYYY-MM-DD or YYYY)
    release_date = safe_get("album_release_date", "")
    year = ""
    if release_date:
        year = str(release_date)[:4]

    # Get genres - prefer artist_genres, fallback to album_genres
    genre = safe_get("artist_genres", "")
    if not genre:
        genre = safe_get("album_genres", "")

    return {
        'title': safe_get("track_name"),
        'artist': clean_artist_string(safe_get("artist_name(s)")),
        'album_artist': clean_artist_string(safe_get("album_artist_name(s)")),
        'album': safe_get("album_name"),
        'year': year,
        'track_number': safe_get("track_number"),
        'disc_number': safe_get("disc_number"),
        'genre': genre,
        'isrc': safe_get("isrc"),
        'label': safe_get("label"),
        'copyright': safe_get("copyrights"),
        'album_art_url': safe_get("album_image_url"),
    }


# ---------------------------
#  5. YT-DLP Download Functions
# ---------------------------

class DownloadProgress:
    """Track download progress for logging."""
    def __init__(self, track_name: str):
        self.track_name = track_name
        self.started = False

    def hook(self, d):
        if d['status'] == 'downloading':
            pct = d.get('_percent_str', '?%').strip()
            speed = d.get('_speed_str', '?').strip()
            eta = d.get('_eta_str', '?').strip()
            print(f"\r       Progress: {pct} at {speed} (ETA: {eta})       ", end='', flush=True)
        elif d['status'] == 'finished':
            print(f"\r       Download complete, converting to {AUDIO_FORMAT}...                    ")


# Global cache for cookie settings (extracted once, reused throughout session)
_cached_cookie_settings = None
_session_cookie_file = None  # Temp file for cached cookies


def extract_and_cache_cookies() -> str | None:
    """
    Extract cookies from browser ONCE and save to a temp file.
    Returns path to the cached cookie file, or None if extraction fails.
    """
    global _session_cookie_file

    if _session_cookie_file and Path(_session_cookie_file).exists():
        return _session_cookie_file

    if not COOKIES_FROM_BROWSER:
        return None

    try:
        import tempfile

        # Create temp cookie file in the project's logs directory for easy debugging
        cookie_cache_dir = folder_path / "logs" / ".cookie_cache"
        cookie_cache_dir.mkdir(parents=True, exist_ok=True)
        temp_cookie_path = cookie_cache_dir / f"session_cookies_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"

        print(f"[Cookies] Extracting from {COOKIES_FROM_BROWSER}...")

        # Use yt-dlp to extract and save cookies
        opts = {
            'cookiesfrombrowser': (COOKIES_FROM_BROWSER,),
            'cookiefile': str(temp_cookie_path),
            'quiet': True,
            'skip_download': True,
            'simulate': True,
        }

        with yt_dlp.YoutubeDL(opts) as ydl:
            # Just initializing extracts and saves cookies
            pass

        if temp_cookie_path.exists():
            _session_cookie_file = str(temp_cookie_path)
            cookie_count = sum(1 for line in open(temp_cookie_path) if not line.startswith('#') and line.strip())
            print(f"[Cookies] Cached {cookie_count} cookies to: {temp_cookie_path.name}")
            return _session_cookie_file
        else:
            print(f"[Cookies] WARNING: Cookie extraction failed")
            return None

    except Exception as e:
        print(f"[Cookies] ERROR extracting cookies: {e}")
        return None


def get_cookie_settings(force_refresh: bool = False) -> dict:
    """
    Determine cookie settings based on configuration.
    Returns dict with 'cookiefile' key (using cached cookies), or empty dict.

    Cookies are extracted ONCE from browser and cached for the entire session.
    Use force_refresh=True to re-extract (e.g., if cookies expired).
    """
    global _cached_cookie_settings

    # Return cached settings if available (unless refresh requested)
    if _cached_cookie_settings is not None and not force_refresh:
        return _cached_cookie_settings

    settings = {}

    # Try to use cached session cookies (extracted from browser once)
    if _session_cookie_file and Path(_session_cookie_file).exists():
        settings['cookiefile'] = _session_cookie_file
        _cached_cookie_settings = settings
        return settings

    # Fall back to configured cookies file (if exists)
    if COOKIES_FILE and Path(COOKIES_FILE).exists():
        settings['cookiefile'] = str(COOKIES_FILE)
        _cached_cookie_settings = settings
        return settings

    # Last resort: extract from browser each time (slower but works)
    if COOKIES_FROM_BROWSER:
        settings['cookiesfrombrowser'] = (COOKIES_FROM_BROWSER,)
        _cached_cookie_settings = settings
        return settings

    _cached_cookie_settings = settings
    return settings


def build_extractor_args() -> dict:
    """
    Build extractor arguments for YouTube.
    Returns dict for yt-dlp 'extractor_args' option.
    """
    youtube_args = []

    # Player client selection
    if YOUTUBE_PLAYER_CLIENTS:
        youtube_args.append(f"player_client={YOUTUBE_PLAYER_CLIENTS}")

    # PO Token for bypassing 403 errors
    if YOUTUBE_PO_TOKEN:
        youtube_args.append(f"po_token={YOUTUBE_PO_TOKEN}")

    # Visitor data (alternative authentication)
    if YOUTUBE_VISITOR_DATA:
        youtube_args.append(f"visitor_data={YOUTUBE_VISITOR_DATA}")

    if youtube_args:
        return {'youtube': youtube_args}
    return {}


def get_yt_dlp_options(output_template: str, progress_hook=None) -> dict:
    """
    Build comprehensive yt-dlp options dictionary.
    Includes all authentication, network, and anti-detection settings.
    """
    opts = {
        # Audio extraction
        'format': 'bestaudio/best',
        'extract_audio': True,
        'audio_format': AUDIO_FORMAT,
        'audio_quality': AUDIO_QUALITY,

        # Output
        'outtmpl': output_template,
        'restrictfilenames': False,  # Allow unicode in filenames

        # No YouTube metadata/thumbnails - we'll use our own
        'writethumbnail': False,
        'embedthumbnail': False,
        'addmetadata': False,  # Don't add YouTube metadata

        # Postprocessors - only audio extraction
        'postprocessors': [{
            'key': 'FFmpegExtractAudio',
            'preferredcodec': AUDIO_FORMAT,
            'preferredquality': AUDIO_QUALITY,
        }],

        # Quiet mode with custom progress
        'quiet': False,
        'no_warnings': False,
        'noprogress': False,

        # Safety options
        'ignoreerrors': False,
        'no_overwrites': True,  # Don't re-download existing files

        # ===== NETWORK OPTIONS =====
        'retries': HTTP_RETRIES,
        'fragment_retries': FRAGMENT_RETRIES,
        'extractor_retries': EXTRACTOR_RETRIES,
        'file_access_retries': FILE_ACCESS_RETRIES,
        'skip_unavailable_fragments': True,

        # Socket and connection settings
        'socket_timeout': SOCKET_TIMEOUT,

        # ===== ANTI-DETECTION / WORKAROUND OPTIONS =====
        # Sleep intervals (randomized to appear more human)
        'sleep_interval': 1,
        'max_sleep_interval': 3,
    }

    # ===== COOKIE AUTHENTICATION =====
    cookie_settings = get_cookie_settings()
    opts.update(cookie_settings)

    # ===== YOUTUBE EXTRACTOR ARGS =====
    extractor_args = build_extractor_args()
    if extractor_args:
        opts['extractor_args'] = extractor_args

    # ===== NETWORK: IPv4/IPv6 =====
    if FORCE_IPV4:
        opts['source_address'] = '0.0.0.0'  # Forces IPv4
    elif SOURCE_ADDRESS:
        opts['source_address'] = SOURCE_ADDRESS

    # ===== USER AGENT =====
    if USER_AGENT:
        opts['http_headers'] = {'User-Agent': USER_AGENT}

    # ===== GEO-BYPASS =====
    if GEO_BYPASS:
        opts['geo_bypass'] = True
        if GEO_BYPASS_COUNTRY:
            opts['geo_bypass_country'] = GEO_BYPASS_COUNTRY

    # ===== AGE LIMIT =====
    if AGE_LIMIT is not None:
        opts['age_limit'] = AGE_LIMIT

    # ===== IMPERSONATION (requires curl_cffi) =====
    if IMPERSONATE_BROWSER:
        opts['impersonate'] = IMPERSONATE_BROWSER

    # ===== PROGRESS HOOK =====
    if progress_hook:
        opts['progress_hooks'] = [progress_hook]

    return opts


def download_from_url(url: str, output_template: str, track_name: str, expected_duration_ms: float) -> tuple:
    """
    Download a track from a specific YouTube URL.
    Returns: (success, status_message, actual_duration_seconds, video_title)
    """
    progress = DownloadProgress(track_name)
    opts = get_yt_dlp_options(output_template, progress.hook)

    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            # First, extract info to validate duration
            print(f"       Fetching video info...")
            info = ydl.extract_info(url, download=False)

            if not info:
                print(f"       ERROR: Could not fetch video info")
                return False, "no_info", 0, ""

            actual_duration = info.get('duration', 0)
            video_title = info.get('title', 'Unknown')

            print(f"       Video: {video_title}")
            print(f"       Duration: {format_seconds(actual_duration)} (expected: {format_duration(expected_duration_ms)})")

            # Validate duration
            if not duration_matches(expected_duration_ms, actual_duration):
                print(f"       WARNING: Duration mismatch!")
                return False, f"duration_mismatch", actual_duration, video_title

            # Download
            print(f"       Starting download...")
            ydl.download([url])
            return True, "downloaded", actual_duration, video_title

    except yt_dlp.utils.DownloadError as e:
        error_msg = str(e).lower()
        print(f"       Download error: {str(e)[:80]}")

        # Categorize errors for proper retry logic
        if "sign in" in error_msg and "bot" in error_msg:
            # "Sign in to confirm you're not a bot" - needs cookies
            return False, "sign_in_required", 0, ""
        elif "sign in" in error_msg:
            # Generic sign-in required
            return False, "sign_in_required", 0, ""
        elif "private" in error_msg:
            return False, "private_video", 0, ""
        elif "unavailable" in error_msg or "removed" in error_msg:
            return False, "unavailable", 0, ""
        elif "age" in error_msg:
            return False, "age_restricted", 0, ""
        elif "copyright" in error_msg:
            return False, "copyright_blocked", 0, ""
        elif "403" in error_msg or "forbidden" in error_msg:
            return False, "http_403_po_token_needed", 0, ""
        elif "429" in error_msg or "too many" in error_msg:
            return False, "rate_limited", 0, ""
        else:
            return False, "download_error", 0, ""

    except Exception as e:
        print(f"       Exception: {str(e)[:80]}")
        return False, f"error", 0, ""


def search_and_download(search_query: str, output_template: str, track_name: str, expected_duration_ms: float) -> tuple:
    """
    Search YouTube for a track and download the best match.
    Returns: (success, status_message, found_url, actual_duration_seconds)
    """
    progress = DownloadProgress(track_name)

    # First, search and get candidates (include cookies for authenticated search)
    search_opts = {
        'quiet': False,
        'no_warnings': False,
        'extract_flat': True,
        'default_search': 'ytsearch5',  # Get top 5 results
        'socket_timeout': SOCKET_TIMEOUT,
        'sleep_interval_requests': SLEEP_REQUESTS,
    }

    # Add cookie settings to search (helps with personalized/restricted results)
    search_opts.update(get_cookie_settings())

    # Add extractor args
    extractor_args = build_extractor_args()
    if extractor_args:
        search_opts['extractor_args'] = extractor_args

    try:
        print(f"       Searching YouTube: '{search_query}'...")

        with yt_dlp.YoutubeDL(search_opts) as ydl:
            search_results = ydl.extract_info(f"ytsearch5:{search_query}", download=False)

        if not search_results or 'entries' not in search_results:
            print(f"       No search results found")
            return False, "no_search_results", "", 0

        entries = [e for e in search_results['entries'] if e]
        if not entries:
            print(f"       No valid entries in search results")
            return False, "no_search_results", "", 0

        print(f"       Found {len(entries)} results, analyzing...")

        # Find best match based on duration
        best_match = None
        best_duration_diff = float('inf')
        expected_seconds = expected_duration_ms / 1000 if expected_duration_ms and expected_duration_ms > 0 else None

        for idx, entry in enumerate(entries):
            entry_duration = entry.get('duration', 0)
            entry_title = entry.get('title', 'Unknown')[:50]

            if expected_seconds:
                duration_diff = abs(entry_duration - expected_seconds)
                print(f"         [{idx+1}] {entry_title}... [{format_seconds(entry_duration)}] (diff: {duration_diff:.0f}s)")

                if duration_diff < best_duration_diff:
                    best_duration_diff = duration_diff
                    best_match = entry
            else:
                # No expected duration, take first result
                print(f"         [{idx+1}] {entry_title}... [{format_seconds(entry_duration)}] (selected - no expected duration)")
                best_match = entry
                break

        if not best_match:
            print(f"       No valid match found")
            return False, "no_valid_match", "", 0

        video_url = f"https://www.youtube.com/watch?v={best_match['id']}"
        actual_duration = best_match.get('duration', 0)

        # Validate duration before downloading
        if expected_seconds and not duration_matches(expected_duration_ms, actual_duration):
            print(f"       Best match duration differs too much from expected!")
            return False, f"search_duration_mismatch", video_url, actual_duration

        print(f"       Selected: {best_match.get('title', 'Unknown')}")
        print(f"       URL: {video_url}")

        # Now download with full options
        download_opts = get_yt_dlp_options(output_template, progress.hook)

        with yt_dlp.YoutubeDL(download_opts) as ydl:
            ydl.download([video_url])

        return True, "search_downloaded", video_url, actual_duration

    except Exception as e:
        print(f"       Search error: {str(e)[:80]}")
        return False, f"search_error", "", 0


# ---------------------------
#  6. Main Download Loop
# ---------------------------

def main():
    # Set up logging to file + console (fail-safe: won't break script)
    log_file = None
    run_log_dir = None
    try:
        run_timestamp = datetime.now().strftime('%Y-%m-%d_%H%M%S')
        run_log_dir = folder_path / "logs" / run_timestamp
        run_log_dir.mkdir(parents=True, exist_ok=True)
        log_file = open(run_log_dir / "output.log", "w", encoding="utf-8")
        sys.stdout = Tee(sys.__stdout__, log_file)
        sys.stderr = Tee(sys.__stderr__, log_file)
    except Exception as e:
        print(f"[Warning] Could not set up logging: {e}")
        print("[Warning] Continuing without file logging...")
        log_file = None

    print_separator("=")
    if run_log_dir:
        print(f"Log file: {run_log_dir / 'output.log'}")
        print_separator("=")
    print("YT-DLP MUSIC DOWNLOADER")
    print_separator("=")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Max runtime: {MAX_RUNTIME_MINUTES} minutes")
    print(f"Output directory: {download_dir}")
    print(f"Save frequency: Every {SAVE_EVERY_N} download(s)")
    print(f"Metadata embedding: {'ENABLED' if EMBED_METADATA and MUTAGEN_AVAILABLE else 'DISABLED'}")
    print(f"Album art embedding: {'ENABLED' if EMBED_ALBUM_ART and MUTAGEN_AVAILABLE else 'DISABLED'}")

    # Extract and cache cookies ONCE at startup (instead of every download)
    print()
    print_separator("-")
    print("COOKIE EXTRACTION")
    print_separator("-")
    extract_and_cache_cookies()

    # Show authentication status
    print()
    print_separator("-")
    print("AUTHENTICATION STATUS")
    print_separator("-")
    cookie_settings = get_cookie_settings()
    if 'cookiefile' in cookie_settings:
        cookie_source = cookie_settings['cookiefile']
        if _session_cookie_file and cookie_source == _session_cookie_file:
            print(f"  Cookies: CACHED from {COOKIES_FROM_BROWSER} (extracted once)")
        else:
            print(f"  Cookies: FROM FILE ({cookie_source})")
        print(f"           Age-restricted & private videos: ENABLED")
    elif 'cookiesfrombrowser' in cookie_settings:
        print(f"  Cookies: FROM BROWSER ({COOKIES_FROM_BROWSER}) - extracting each time")
        print(f"           Age-restricted & private videos: ENABLED")
    else:
        print(f"  Cookies: NOT CONFIGURED")
        print(f"           WARNING: May encounter 'Sign in to confirm you're not a bot' errors")
        print(f"           WARNING: Age-restricted & private videos will FAIL")
        if COOKIES_FROM_BROWSER:
            print(f"           Configured browser '{COOKIES_FROM_BROWSER}' not accessible")
        if COOKIES_FILE:
            print(f"           Cookies file not found: {COOKIES_FILE}")

    if YOUTUBE_PLAYER_CLIENTS:
        print(f"  YouTube clients: {YOUTUBE_PLAYER_CLIENTS}")
    if YOUTUBE_PO_TOKEN:
        print(f"  PO Token: CONFIGURED")
    if GEO_BYPASS:
        print(f"  Geo-bypass: ENABLED")
    print_separator("-")
    print()

    # Create download directory if it doesn't exist
    download_dir.mkdir(parents=True, exist_ok=True)
    print(f"Download directory ready: {download_dir}")

    # Load data - single CSV workflow (same file for input and output)
    print(f"\nLoading data from: {input_csv}")

    df = pd.read_csv(input_csv, encoding="UTF-8")

    # Ensure required columns exist
    for col in ["downloaded", "download_status", "download_date", "actual_duration", "searched_url", "metadata_embedded", "yt_url_origin"]:
        if col not in df.columns:
            df[col] = ""

    # Convert columns to string type and handle NaN
    for col in ["downloaded", "download_status", "download_date", "searched_url", "yt_url", "status", "metadata_embedded"]:
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str)

    print(f"\nTotal tracks in library: {len(df)}")

    # Count tracks with YouTube URLs
    has_url = df["yt_url"].str.strip() != ""
    print(f"Tracks with YouTube URL: {has_url.sum()}")

    # Count already downloaded
    already_downloaded = (df["downloaded"] == "yes").sum()
    print(f"Already downloaded: {already_downloaded}")

    if already_downloaded > 0:
        print(">>> RESUMING from previous session <<<")

    # Identify tracks to process
    # Skip already downloaded and truly permanent failures

    # Truly permanent failures (video doesn't exist or blocked - cookies won't help)
    permanent_failures = ["unavailable", "copyright_blocked"]

    # Cookie-dependent failures (might work NOW with cookies enabled)
    # These will be retried if cookies are configured
    cookie_dependent_failures = ["private_video", "age_restricted", "sign_in_required"]

    # Determine which failures to skip
    if cookie_settings:
        # Cookies enabled - only skip truly permanent failures
        # This means previously failed private/age-restricted will be RETRIED
        skip_statuses = permanent_failures
        cookie_retry_count = df["download_status"].isin(cookie_dependent_failures).sum()
        if cookie_retry_count > 0:
            print(f"\n>>> COOKIES ENABLED: Will retry {cookie_retry_count} previously failed tracks <<<")
            print(f"    (private_video, age_restricted, sign_in_required)")
    else:
        # No cookies - skip both permanent and cookie-dependent failures
        skip_statuses = permanent_failures + cookie_dependent_failures

    # A track needs processing if:
    # 1. Not yet downloaded (downloaded != "yes")
    # 2. Not a permanent/skipped failure status
    needs_processing = (
        (df["downloaded"].str.strip() != "yes") &
        (~df["download_status"].isin(skip_statuses))
    )

    # Tracks that can be downloaded:
    # - Has URL: direct download
    # - No URL but has track info: can try yt-dlp search (including no_yt status)
    can_download = (
        has_url |  # Has URL: can download directly
        (df["track_name"].str.strip() != "")  # Has track name: can try search
    )

    todo_mask = needs_processing & can_download

    # Prioritize tracks with URLs first, then those needing search
    todo_with_url = df.index[todo_mask & has_url].tolist()
    todo_without_url = df.index[todo_mask & ~has_url].tolist()
    todo_idx = todo_with_url + todo_without_url

    total_todo = len(todo_idx)
    print(f"\nTracks to process this session: {total_todo}")
    print(f"  - With URL (direct download): {len(todo_with_url)}")
    print(f"  - Without URL (will search): {len(todo_without_url)}")

    if todo_idx:
        first_idx = todo_idx[0]
        first_track = df.at[first_idx, "track_name"]
        first_artist = get_primary_artist(df.at[first_idx, "artist_name(s)"])
        first_status = df.at[first_idx, "download_status"]
        print(f"\nResuming from: {first_artist} - {first_track}")
        if first_status:
            print(f"Previous status: '{first_status}'")

    print()
    print_separator("=")

    if not todo_idx:
        print("\nNothing to download! All tracks already processed.")
        print_stats(df, 0, 0, 0, 0, time.time())
        return

    # Start processing
    start_time = time.time()
    max_runtime_seconds = MAX_RUNTIME_MINUTES * 60
    processed = 0
    downloaded = 0
    failed = 0
    searched = 0
    consecutive_failures = 0  # Track failures for rate limit detection
    total_downloaded_session = 0  # For long pause logic

    for n, i in enumerate(todo_idx, start=1):
        elapsed = time.time() - start_time

        # Check runtime limit
        if elapsed > max_runtime_seconds:
            print(f"\n{'='*60}")
            print(f"MAX RUNTIME REACHED ({MAX_RUNTIME_MINUTES} minutes)")
            print(f"Stopping gracefully. Re-run to continue.")
            print(f"{'='*60}")
            break

        # Check download limit
        if MAX_DOWNLOADS_THIS_RUN and processed >= MAX_DOWNLOADS_THIS_RUN:
            print(f"\n{'='*60}")
            print(f"DOWNLOAD LIMIT REACHED ({MAX_DOWNLOADS_THIS_RUN})")
            print(f"{'='*60}")
            break

        # Get track info
        row = df.loc[i]
        track_name = row.get("track_name", "Unknown")
        artist_names = row.get("artist_name(s)", "Unknown Artist")
        album_name = row.get("album_name", "Unknown Album")
        yt_url = row.get("yt_url", "").strip()
        duration_ms = row.get("track_duration(ms)", 0)

        # Convert duration to numeric if it's a string
        try:
            duration_ms = float(duration_ms) if duration_ms else 0
        except (ValueError, TypeError):
            duration_ms = 0

        # Print track info
        print()
        print_separator("-")
        print(f"[{n}/{total_todo}] Processing track (row index: {i})")
        print_separator("-")
        print(f"   Track:    {track_name}")
        print(f"   Artist:   {get_primary_artist(artist_names)}")
        print(f"   Album:    {album_name}")
        print(f"   Duration: {format_duration(duration_ms)}")

        # Build output path
        output_template = get_output_template(artist_names, album_name, track_name)
        expected_file = Path(output_template.replace("%(ext)s", AUDIO_FORMAT))
        print(f"   Output:   {expected_file}")

        # Check if file already exists
        if expected_file.exists():
            print(f"\n       FILE ALREADY EXISTS - Marking as done")
            df.at[i, "downloaded"] = "yes"
            df.at[i, "download_status"] = "already_exists"
            df.at[i, "download_date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            # Embed metadata if not already done
            if EMBED_METADATA and MUTAGEN_AVAILABLE and df.at[i, "metadata_embedded"] != "yes":
                print(f"       Embedding metadata to existing file...")
                metadata = extract_metadata_from_row(row)
                if embed_metadata(str(expected_file), metadata):
                    df.at[i, "metadata_embedded"] = "yes"
                    print(f"       Metadata embedded successfully!")

            atomic_save_csv(df, output_csv)
            processed += 1
            downloaded += 1
            continue

        # Create directory
        expected_file.parent.mkdir(parents=True, exist_ok=True)

        # Download
        success = False
        status = ""
        found_url = ""
        actual_duration = 0

        if yt_url:
            # Has URL - direct download
            print(f"\n   Method:   Direct download from URL")
            print(f"   URL:      {yt_url}")
            print()
            success, status, actual_duration, video_title = download_from_url(
                yt_url, output_template, track_name, duration_ms
            )

            # FALLBACK: If URL fails for recoverable reasons, try YouTube search
            # These failures might be resolved by finding an alternative upload
            searchable_failures = [
                "duration_mismatch",      # Wrong video at URL
                "private_video",          # Video went private
                "unavailable",            # Video deleted/removed
                "http_403_po_token_needed",  # Access denied, try different upload
                "download_error",         # Generic error, might find alternative
                "copyright_blocked",      # Might find different upload
            ]

            if not success and status in searchable_failures:
                print(f"\n       URL failed ({status}) - trying YouTube search fallback...")
                searched += 1
                search_query = build_search_query(track_name, artist_names)
                print(f"       Search query: '{search_query}'")

                # Sleep before search to avoid rate limiting
                sleep_time = random.uniform(*SLEEP_BETWEEN_SEARCHES)
                print(f"       Waiting {sleep_time:.1f}s before search...")
                time.sleep(sleep_time)

                success, search_status, found_url, actual_duration = search_and_download(
                    search_query, output_template, track_name, duration_ms
                )

                # Update status to reflect search attempt
                if success:
                    status = f"search_fallback_from_{status}"
                else:
                    status = f"{status}_search_failed"

                if found_url:
                    df.at[i, "searched_url"] = found_url
                    if success:
                        # Update URL with the correct one from search
                        df.at[i, "yt_url"] = found_url
                        df.at[i, "yt_url_origin"] = "yt_search_fallback"
                        print(f"       Updated URL in database: {found_url}")

        else:
            # No URL - search YouTube
            searched += 1
            search_query = build_search_query(track_name, artist_names)
            print(f"\n   Method:   YouTube search")
            print(f"   Query:    '{search_query}'")
            print()

            # Sleep before search to avoid rate limiting
            sleep_time = random.uniform(*SLEEP_BETWEEN_SEARCHES)
            print(f"       Waiting {sleep_time:.1f}s before search...")
            time.sleep(sleep_time)

            success, status, found_url, actual_duration = search_and_download(
                search_query, output_template, track_name, duration_ms
            )

            if found_url:
                df.at[i, "searched_url"] = found_url
                # Also save to main yt_url column if download was successful
                if success:
                    df.at[i, "yt_url"] = found_url
                    df.at[i, "yt_url_origin"] = "yt_search"
                    print(f"       Saved URL to database: {found_url}")

        # Update DataFrame
        df.at[i, "download_status"] = status
        df.at[i, "download_date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        if actual_duration:
            df.at[i, "actual_duration"] = str(int(actual_duration))

        if success:
            df.at[i, "downloaded"] = "yes"
            downloaded += 1
            total_downloaded_session += 1
            consecutive_failures = 0  # Reset on success
            print(f"\n       >>> DOWNLOAD SUCCESS! <<<")

            # Embed metadata during sleep time
            if EMBED_METADATA and MUTAGEN_AVAILABLE:
                print(f"\n       Embedding Spotify metadata (ID3v2.3)...")
                metadata = extract_metadata_from_row(row)

                # Print metadata being embedded
                print(f"         Title:       {metadata.get('title', 'N/A')}")
                print(f"         Artist:      {metadata.get('artist', 'N/A')}")
                print(f"         Album:       {metadata.get('album', 'N/A')}")
                print(f"         Year:        {metadata.get('year', 'N/A')}")
                print(f"         Track:       {metadata.get('track_number', 'N/A')}")
                print(f"         Genre:       {metadata.get('genre', 'N/A')[:30] if metadata.get('genre') else 'N/A'}...")
                print(f"         Album Art:   {'Yes' if metadata.get('album_art_url') else 'No'}")

                if embed_metadata(str(expected_file), metadata):
                    df.at[i, "metadata_embedded"] = "yes"
                    print(f"\n       >>> METADATA EMBEDDED! <<<")
                else:
                    df.at[i, "metadata_embedded"] = "failed"
                    print(f"\n       >>> METADATA FAILED <<<")
        else:
            df.at[i, "downloaded"] = "no"
            failed += 1
            print(f"\n       >>> FAILED: {status} <<<")

            # Only count network/rate-limit errors toward consecutive failures
            # These are NOT rate limit issues (don't count them):
            non_rate_limit_errors = [
                "duration_mismatch", "search_duration_mismatch",
                "private_video", "unavailable", "age_restricted",
                "copyright_blocked", "no_search_results", "no_valid_match"
            ]

            if status not in non_rate_limit_errors:
                consecutive_failures += 1
                # Check for possible rate limiting / IP ban
                if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                    print(f"\n{'='*60}")
                    print(f"WARNING: {consecutive_failures} consecutive network failures detected!")
                    print(f"Possible rate limiting or IP ban.")
                    print(f"Pausing for {RATE_LIMIT_PAUSE_MINUTES} minutes...")
                    print(f"{'='*60}")
                    atomic_save_csv(df, output_csv)  # Save before pause
                    time.sleep(RATE_LIMIT_PAUSE_MINUTES * 60)
                    consecutive_failures = 0  # Reset after pause
                    print(f"Resuming downloads...")
            else:
                # Non-network error - reset consecutive counter
                consecutive_failures = 0

        processed += 1

        # Save progress (every SAVE_EVERY_N downloads)
        if processed % SAVE_EVERY_N == 0:
            atomic_save_csv(df, output_csv)
            print(f"\n       [Checkpoint saved to {output_csv.name}]")

        # Progress bar (time-based)
        elapsed = time.time() - start_time
        progress_pct = min(1.0, elapsed / max_runtime_seconds)
        bar_len = 30
        filled = int(bar_len * progress_pct)
        bar = "#" * filled + "-" * (bar_len - filled)
        remaining_min = (max_runtime_seconds - elapsed) / 60

        print(f"\n   Time: [{bar}] {progress_pct*100:.1f}% ({remaining_min:.0f} min remaining)")

        # Sleep between successful downloads
        if success:
            sleep_time = random.uniform(*SLEEP_BETWEEN_DOWNLOADS)
            print(f"   Sleeping {sleep_time:.1f}s before next download...")
            time.sleep(sleep_time)

            # Take a longer break every N downloads to avoid detection
            if total_downloaded_session > 0 and total_downloaded_session % LONG_PAUSE_EVERY_N == 0:
                long_pause = random.uniform(*LONG_PAUSE_RANGE)
                print(f"\n   {'='*50}")
                print(f"   Taking a longer break ({long_pause:.0f}s) after {total_downloaded_session} downloads...")
                print(f"   This helps avoid rate limiting.")
                print(f"   {'='*50}")
                time.sleep(long_pause)

    # Final save
    atomic_save_csv(df, output_csv)
    print(f"\nProgress saved to: {output_csv}")

    # Summary
    print_stats(df, processed, downloaded, failed, searched, start_time)

    print("\nTo continue downloading, simply run this script again!")
    print(f"Command: python {Path(__file__).name}")

    # Cleanup: restore stdout/stderr and close log file
    try:
        sys.stdout = sys.__stdout__
        sys.stderr = sys.__stderr__
        if log_file:
            log_file.close()
            print(f"Log saved to: {run_log_dir / 'output.log'}")
    except:
        pass


if __name__ == "__main__":
    main()
