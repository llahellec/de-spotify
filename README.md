# De-Spotify

A collection of Python scripts to enrich Spotify playlist data with YouTube links and metadata from external sources (Songstats and Discogs).

## Main Scripts

- **songstats.py** - Main script to fetch YouTube links from Songstats using ISRC codes
- **discogs.py** - Main script to fetch YouTube links from Discogs API for albums
- **merge_yt_urls.py** - Merge songstats and discogs results into a master file (prioritizes songstats)
- **discogs_single.py** - Single album fetcher for Discogs

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Configure environment variables:
   - Copy `.env.example` to `.env`
   - Add your Discogs API credentials (get them from https://www.discogs.com/settings/developers)

```bash
cp .env.example .env
# Edit .env with your credentials
```

3. Place your Spotify playlist CSV files in the `data/spotify_playlists/` directory

## Usage

### Step 1: Fetch YouTube links via Songstats
```bash
python code/songstats.py
```
This creates `liked_yt_songstats.csv` with YouTube links from Songstats.

### Step 2: Fetch YouTube links via Discogs
```bash
python code/discogs.py
```
This creates `liked_yt_discogs.csv` with YouTube links from Discogs.

### Step 3: Merge both sources into master file
```bash
python code/merge_yt_urls.py
```
This creates `liked_master.csv` which:
- Prioritizes YouTube links from Songstats
- Fills missing links with Discogs data (marked as `discogs_fallback`)
- Provides maximum URL coverage from both sources

To verify the merge quality:
```bash
python code/verify_merge.py
```

## Features

- Resumable processing - scripts can be stopped and restarted
- Progress tracking with status columns
- Rate limiting to respect API limits
- Automatic retries and error handling
- Multiple fallback strategies for finding YouTube links

---

## Step 4: Download Music with yt-dlp

### Prerequisites

1. **Install yt-dlp:**
```bash
pip install yt-dlp
```

2. **Install FFmpeg** (required for audio extraction):
   - **Windows:** Download from https://ffmpeg.org/download.html or use `winget install ffmpeg`
   - **Linux:** `sudo apt install ffmpeg`
   - **Mac:** `brew install ffmpeg`

### Usage

```bash
python code/yt_download.py
```

This script will:
- Load `liked_master.csv` (or resume from `liked_downloaded.csv`)
- Download songs as MP3 at best quality (320kbps when available)
- Organize files as: `downloads/Artist/Album/Track.mp3`
- Search YouTube automatically for tracks without URLs
- Validate song duration to ensure correct matches
- Save progress periodically (resumable)

### Configuration

Edit the following settings in `yt_download.py`:

| Setting | Default | Description |
|---------|---------|-------------|
| `MAX_RUNTIME_MINUTES` | 120 | Auto-stop after this time |
| `SAVE_EVERY_N` | 10 | Save CSV every N downloads |
| `DURATION_TOLERANCE_PERCENT` | 15 | Duration matching tolerance |
| `SLEEP_BETWEEN_DOWNLOADS` | (3, 6) | Random delay range (seconds) |
| `AUDIO_QUALITY` | "0" | Best quality (0-10 scale) |

### Output

- **Downloads:** `downloads/Artist/Album/Track.mp3`
- **Progress CSV:** `data/spotify_playlists/main/liked_downloaded.csv`

### PO Token (If YouTube blocks downloads)

If you encounter HTTP 403 errors, you may need to provide a PO Token. See `doc/PO Token Guide.md` for instructions.

---

## Data

All data files are stored in the `data/` directory and are excluded from git to protect privacy.
