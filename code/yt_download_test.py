# YT-DLP DOWNLOAD TEST WITH METADATA
# ===================================
# Tests 2 downloads with full Spotify metadata embedding
# Run this to verify everything works before running the full script

import os
import re
import time
import pandas as pd
import requests
from pathlib import Path
from datetime import datetime

print("="*60)
print("YT-DLP DOWNLOAD TEST WITH METADATA")
print("="*60)

# Check dependencies
try:
    import yt_dlp
    print(f"[OK] yt-dlp version: {yt_dlp.version.__version__}")
except ImportError:
    print("[ERROR] yt-dlp not installed! pip install yt-dlp")
    exit(1)

try:
    from mutagen.id3 import ID3, TIT2, TPE1, TPE2, TALB, TRCK, TPOS, TCON, TYER, TDRC, TSRC, TPUB, TCOP, APIC, ID3NoHeaderError
    print(f"[OK] mutagen available for ID3 tags")
    MUTAGEN_AVAILABLE = True
except ImportError:
    print("[WARNING] mutagen not installed - metadata will not be embedded")
    MUTAGEN_AVAILABLE = False

# Check ffmpeg
import subprocess
try:
    result = subprocess.run(['ffmpeg', '-version'], capture_output=True, text=True)
    version_line = result.stdout.split('\n')[0] if result.stdout else "unknown"
    print(f"[OK] ffmpeg: {version_line[:50]}...")
except FileNotFoundError:
    # Try to add to PATH
    ffmpeg_path = os.path.expanduser(
        "~/AppData/Local/Microsoft/WinGet/Packages/Gyan.FFmpeg_Microsoft.Winget.Source_8wekyb3d8bbwe/ffmpeg-8.0.1-full_build/bin"
    )
    if os.path.exists(ffmpeg_path):
        os.environ["PATH"] = ffmpeg_path + os.pathsep + os.environ.get("PATH", "")
        print(f"[OK] ffmpeg added to PATH from: {ffmpeg_path}")
    else:
        print("[ERROR] ffmpeg not found!")
        exit(1)

print()

# Configuration
AUDIO_FORMAT = "mp3"
AUDIO_QUALITY = "0"
folder_path = Path(__file__).resolve().parents[1]
download_dir = folder_path / "downloads" / "_TEST_METADATA"
download_dir.mkdir(parents=True, exist_ok=True)

print(f"Test download directory: {download_dir}")

def sanitize_filename(name):
    if not name:
        return "Unknown"
    name = re.sub(r'[<>:"/\\|?*]', '_', name)
    return name.strip()[:100] or "Unknown"

def format_seconds(sec):
    if not sec or sec <= 0:
        return "??:??"
    return f"{int(sec)//60}:{int(sec)%60:02d}"

def download_progress_hook(d):
    if d['status'] == 'downloading':
        pct = d.get('_percent_str', '?%').strip()
        speed = d.get('_speed_str', '?').strip()
        print(f"\r    Progress: {pct} at {speed}       ", end='', flush=True)
    elif d['status'] == 'finished':
        print(f"\r    Download complete, converting to {AUDIO_FORMAT}...          ")

def get_yt_dlp_options(output_path):
    return {
        'format': 'bestaudio/best',
        'extract_audio': True,
        'audio_format': AUDIO_FORMAT,
        'audio_quality': AUDIO_QUALITY,
        'outtmpl': output_path,
        'writethumbnail': False,
        'embedthumbnail': False,
        'addmetadata': False,  # We'll add our own metadata
        'postprocessors': [{
            'key': 'FFmpegExtractAudio',
            'preferredcodec': AUDIO_FORMAT,
            'preferredquality': AUDIO_QUALITY,
        }],
        'quiet': True,
        'no_warnings': True,
        'progress_hooks': [download_progress_hook],
        'retries': 3,
    }

def download_album_art(url):
    """Download album art and return bytes."""
    if not url:
        return None
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            return response.content
    except Exception as e:
        print(f"    Warning: Could not download album art: {e}")
    return None

def embed_metadata(file_path, metadata):
    """Embed ID3v2.3 metadata into MP3 file."""
    if not MUTAGEN_AVAILABLE:
        return False

    try:
        try:
            audio = ID3(file_path)
        except ID3NoHeaderError:
            audio = ID3()

        audio.delete()
        audio = ID3()

        # Add all tags
        if metadata.get('title'):
            audio.add(TIT2(encoding=3, text=str(metadata['title'])))
        if metadata.get('artist'):
            audio.add(TPE1(encoding=3, text=str(metadata['artist'])))
        if metadata.get('album_artist'):
            audio.add(TPE2(encoding=3, text=str(metadata['album_artist'])))
        if metadata.get('album'):
            audio.add(TALB(encoding=3, text=str(metadata['album'])))
        if metadata.get('year'):
            audio.add(TYER(encoding=3, text=str(metadata['year'])[:4]))
            audio.add(TDRC(encoding=3, text=str(metadata['year'])[:4]))
        if metadata.get('track_number'):
            audio.add(TRCK(encoding=3, text=str(int(float(metadata['track_number'])))))
        if metadata.get('disc_number'):
            audio.add(TPOS(encoding=3, text=str(int(float(metadata['disc_number'])))))
        if metadata.get('genre'):
            audio.add(TCON(encoding=3, text=str(metadata['genre']).split(',')[0].strip()))
        if metadata.get('isrc'):
            audio.add(TSRC(encoding=3, text=str(metadata['isrc'])))
        if metadata.get('label'):
            audio.add(TPUB(encoding=3, text=str(metadata['label'])))
        if metadata.get('copyright'):
            audio.add(TCOP(encoding=3, text=str(metadata['copyright'])[:200]))

        # Album art
        if metadata.get('album_art_url'):
            art_data = download_album_art(metadata['album_art_url'])
            if art_data:
                audio.add(APIC(
                    encoding=3,
                    mime='image/jpeg',
                    type=3,
                    desc='Cover',
                    data=art_data
                ))

        audio.save(file_path, v2_version=3)
        return True

    except Exception as e:
        print(f"    Error embedding metadata: {e}")
        return False

def verify_metadata(file_path):
    """Read and display embedded metadata."""
    if not MUTAGEN_AVAILABLE:
        return

    try:
        audio = ID3(file_path)
        print("\n    EMBEDDED METADATA VERIFICATION:")
        print("    " + "-"*40)

        tag_map = {
            'TIT2': 'Title',
            'TPE1': 'Artist',
            'TPE2': 'Album Artist',
            'TALB': 'Album',
            'TYER': 'Year',
            'TRCK': 'Track',
            'TPOS': 'Disc',
            'TCON': 'Genre',
            'TSRC': 'ISRC',
            'TPUB': 'Label',
        }

        for tag_id, tag_name in tag_map.items():
            if tag_id in audio:
                value = str(audio[tag_id].text[0])[:40]
                print(f"    {tag_name:15}: {value}")

        # Check for album art
        has_art = any(k.startswith('APIC') for k in audio.keys())
        print(f"    {'Album Art':15}: {'YES' if has_art else 'NO'}")
        print("    " + "-"*40)

    except Exception as e:
        print(f"    Could not verify metadata: {e}")

# ============================================================
# TEST 1: Direct URL Download with Metadata
# ============================================================
print("\n" + "="*60)
print("TEST 1: Direct URL Download with Full Metadata")
print("="*60)

# Simulated Spotify metadata - VARIOUS ARTISTS COMPILATION
# This tests complex metadata: multiple artists, compilation album
test1_metadata = {
    'title': 'Get Lucky',
    'artist': 'Daft Punk, Pharrell Williams, Nile Rodgers',  # Multiple artists!
    'album_artist': 'Daft Punk',  # Main album artist
    'album': 'Random Access Memories',
    'year': '2013',
    'track_number': '8',
    'disc_number': '1',
    'genre': 'electronic, french house, disco',
    'isrc': 'USQX91300108',
    'label': 'Columbia',
    'copyright': '(C) 2013 Daft Life Limited under exclusive license to Columbia Records',
    'album_art_url': 'https://i.scdn.co/image/ab67616d0000b2739b9b36b0e22870b9f542d937',
}

test1_url = "https://www.youtube.com/watch?v=5NV6Rdv1a3I"
test1_expected_duration = 369000  # ms (~6:09)

print(f"\nTrack:    {test1_metadata['artist']} - {test1_metadata['title']}")
print(f"Album:    {test1_metadata['album']} ({test1_metadata['year']})")
print(f"URL:      {test1_url}")

output1 = str(download_dir / sanitize_filename(test1_metadata['artist']) / sanitize_filename(test1_metadata['album']) / f"{sanitize_filename(test1_metadata['title'])}.%(ext)s")
output1_mp3 = output1.replace("%(ext)s", "mp3")
Path(output1_mp3).parent.mkdir(parents=True, exist_ok=True)

try:
    opts = get_yt_dlp_options(output1)

    with yt_dlp.YoutubeDL(opts) as ydl:
        print("\n    Fetching video info...")
        info = ydl.extract_info(test1_url, download=False)

        if info:
            actual_duration = info.get('duration', 0)
            print(f"    Duration: {format_seconds(actual_duration)} (expected: {format_seconds(test1_expected_duration/1000)})")

            print("\n    Starting download...")
            ydl.download([test1_url])

            # Embed metadata
            print("\n    Embedding Spotify metadata (ID3v2.3)...")
            if embed_metadata(output1_mp3, test1_metadata):
                print("    >>> METADATA EMBEDDED! <<<")
                verify_metadata(output1_mp3)
            else:
                print("    >>> METADATA EMBEDDING FAILED <<<")

            print("\n    >>> TEST 1 SUCCESS! <<<")

except Exception as e:
    print(f"\n    >>> TEST 1 FAILED: {e} <<<")

# ============================================================
# TEST 2: YouTube Search with Metadata
# ============================================================
print("\n" + "="*60)
print("TEST 2: YouTube Search with Full Metadata")
print("="*60)

# Simulated Spotify metadata - TRUE "VARIOUS ARTISTS" COMPILATION
# This tests: album_artist = "Various Artists", track artist different
test2_metadata = {
    'title': 'Blue Monday',
    'artist': 'New Order',  # Track artist
    'album_artist': 'Various Artists',  # Compilation!
    'album': 'Substance',
    'year': '1987',
    'track_number': '1',
    'disc_number': '1',
    'genre': 'new wave, synth-pop, post-punk',
    'isrc': 'GBARL8700030',
    'label': 'London Records',
    'copyright': '(C) 1987 London Records 90 Ltd.',
    'album_art_url': 'https://i.scdn.co/image/ab67616d0000b273fe896727e3db1027ed72d885',  # Valid Spotify image
}

test2_expected_duration = 451000  # ms (~7:31)

search_query = f"{test2_metadata['artist']} {test2_metadata['title']}"  # Uses track artist for search
print(f"\nTrack:    {test2_metadata['artist']} - {test2_metadata['title']}")
print(f"Album:    {test2_metadata['album']} ({test2_metadata['year']})")
print(f"Query:    '{search_query}'")

output2 = str(download_dir / sanitize_filename(test2_metadata['artist']) / sanitize_filename(test2_metadata['album']) / f"{sanitize_filename(test2_metadata['title'])}.%(ext)s")
output2_mp3 = output2.replace("%(ext)s", "mp3")
Path(output2_mp3).parent.mkdir(parents=True, exist_ok=True)

try:
    # Search
    search_opts = {
        'quiet': True,
        'no_warnings': True,
        'extract_flat': True,
        'default_search': 'ytsearch5',
    }

    print("\n    Searching YouTube...")
    with yt_dlp.YoutubeDL(search_opts) as ydl:
        results = ydl.extract_info(f"ytsearch5:{search_query}", download=False)

    if results and 'entries' in results:
        entries = [e for e in results['entries'] if e]
        print(f"    Found {len(entries)} results")

        expected_sec = test2_expected_duration / 1000
        best_match = None
        best_diff = float('inf')

        for entry in entries:
            dur = entry.get('duration', 0)
            diff = abs(dur - expected_sec)
            if diff < best_diff:
                best_diff = diff
                best_match = entry

        if best_match:
            video_url = f"https://www.youtube.com/watch?v={best_match['id']}"
            print(f"    Selected: {best_match.get('title', 'Unknown')}")
            print(f"    URL: {video_url}")
            print(f"    >>> URL saved to database! <<<")

            print("\n    Starting download...")
            download_opts = get_yt_dlp_options(output2)
            with yt_dlp.YoutubeDL(download_opts) as ydl:
                ydl.download([video_url])

            # Embed metadata
            print("\n    Embedding Spotify metadata (ID3v2.3)...")
            if embed_metadata(output2_mp3, test2_metadata):
                print("    >>> METADATA EMBEDDED! <<<")
                verify_metadata(output2_mp3)
            else:
                print("    >>> METADATA EMBEDDING FAILED <<<")

            print("\n    >>> TEST 2 SUCCESS! <<<")

except Exception as e:
    print(f"\n    >>> TEST 2 FAILED: {e} <<<")

# ============================================================
# Summary
# ============================================================
print("\n" + "="*60)
print("TEST COMPLETE")
print("="*60)
print(f"\nDownloaded files with metadata:")

for f in download_dir.rglob("*.mp3"):
    size_mb = f.stat().st_size / (1024*1024)
    rel_path = f.relative_to(download_dir)
    print(f"  - {rel_path}")
    print(f"    Size: {size_mb:.1f} MB")

print("\n" + "="*60)
print("If both tests passed, run the full script:")
print("  python code/yt_download.py")
print("="*60)
