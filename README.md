# De-Spotify

A collection of Python scripts to enrich Spotify playlist data with YouTube links and metadata from external sources (Songstats and Discogs).

---

## Disclaimer

**IMPORTANT: Please read before using this project.**

### Legal & Ethical Considerations

- **Personal Use Only**: This project is intended for personal, educational, and archival purposes only. Downloading copyrighted content may be illegal in your jurisdiction.
- **Verify Local Laws**: Laws regarding downloading audio from YouTube vary by country. It is **your responsibility** to verify what is legally permitted in your region before using these tools.
- **Respect Copyright**: Only download content you have the right to access. Consider supporting artists by purchasing their music legally.
- **No Warranty**: This software is provided "as is" without any warranty. The authors are not responsible for any misuse or legal consequences.

### Rate Limiting & API Policies

- **Respect Rate Limits**: All scripts include built-in generous delays between requests. **Do not disable or reduce these delays** — they exist to respect the terms of service of external APIs and websites.
- **API Terms of Service**: When using the Discogs API, you must comply with their [Terms of Service](https://www.discogs.com/developers/#page:home,header:home-general-information). Obtain your own API credentials.
- **YouTube ToS**: Downloading from YouTube may violate their Terms of Service. Use at your own risk.
- **Be a Good Citizen**: Excessive scraping can harm services and lead to IP bans. The default delays (3-6 seconds between downloads) are designed to be respectful.

### System Requirements

- Verify that your system has the necessary permissions and software installed (Python, FFmpeg, etc.)
- Some features may behave differently across operating systems (Windows, macOS, Linux)
- Test with a small batch before running large operations

---

## Main Scripts

| Script | Description | Rate Limit |
|--------|-------------|------------|
| **songstats.py** | Fetches YouTube links from Songstats using ISRC codes. Scrapes public Songstats pages to find official YouTube URLs linked to tracks. | 2-4s delay between requests |
| **discogs.py** | Fetches YouTube links from the Discogs API by searching for albums and extracting video URLs from release pages. Requires API credentials. | 1-2s delay (API limit: 60/min) |
| **merge_yt_urls.py** | Merges results from Songstats and Discogs into a master CSV file. Prioritizes Songstats URLs, falls back to Discogs. | N/A (local processing) |
| **verify_merge.py** | Validates the merged CSV and reports statistics on URL coverage. | N/A (local processing) |
| **discogs_single.py** | Fetches YouTube links for a single album from Discogs. Useful for testing or manual lookups. | 1-2s delay |
| **yt_download.py** | Downloads audio from YouTube URLs, embeds Spotify metadata (ID3v2.3), organizes files by Artist/Album. Includes YouTube search fallback. | 3-6s delay between downloads |
| **yt_download_test.py** | Test script for verifying download and metadata embedding works correctly. Tests both direct URL and search functionality. | N/A (test only) |

## Quick Start

```bash
# 1. Clone the repository
git clone https://github.com/llahellec/de-spotify.git
cd de-spotify

# 2. Install Python dependencies
pip install -r requirements.txt

# 3. Install FFmpeg (required for audio conversion)
# Windows: winget install ffmpeg
# Linux:   sudo apt install ffmpeg
# Mac:     brew install ffmpeg

# 4. Set up your environment
cp .env.example .env
# Edit .env with your Discogs API credentials

# 5. Add your Spotify data (see Folder Structure below)

# 6. Run the scripts in order
python code/songstats.py      # Step 1: Get YouTube URLs from Songstats
python code/discogs.py        # Step 2: Get YouTube URLs from Discogs
python code/merge_yt_urls.py  # Step 3: Merge all URLs
python code/yt_download.py    # Step 4: Download music
```

## Folder Structure

After cloning, your project should look like this:

```
de-spotify/
├── code/                           # All Python scripts
├── data/
│   └── spotify_playlists/
│       └── main/
│           ├── liked.csv           # <-- PUT YOUR SPOTIFY EXPORT HERE
│           └── EXAMPLE_STRUCTURE.md # CSV column reference
├── downloads/                      # Downloaded MP3s will go here
├── doc/                            # Documentation
├── .env.example                    # Template for API credentials
├── .env                            # Your API credentials (create this)
└── requirements.txt
```

## Setup Details

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

**Required packages:**
- `pandas` - Data manipulation
- `requests` - HTTP requests
- `beautifulsoup4` - HTML parsing
- `selenium` - Web scraping (for Songstats)
- `python-dotenv` - Environment variables
- `yt-dlp` - YouTube downloading
- `mutagen` - MP3 metadata embedding

### 2. Configure API Credentials

Copy `.env.example` to `.env` and add your Discogs API credentials:

```bash
cp .env.example .env
```

Get your Discogs credentials at: https://www.discogs.com/settings/developers

### 3. Prepare Your Spotify Data

Export your Spotify library using [Exportify](https://exportify.net/) or similar tool.

Place the CSV file in: `data/spotify_playlists/main/liked.csv`

See `data/spotify_playlists/main/EXAMPLE_STRUCTURE.md` for the expected CSV format.

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

> **Warning**: Downloading audio from YouTube may violate YouTube's Terms of Service and copyright laws in your jurisdiction. This feature is provided for educational purposes. Verify your local laws before use.

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
| `MAX_RUNTIME_MINUTES` | 120 | Auto-stop after this time (prevents long unattended runs) |
| `SAVE_EVERY_N` | 1 | Save CSV every N downloads (1 = fully resumable) |
| `DURATION_TOLERANCE_PERCENT` | 15 | Duration matching tolerance for YouTube search validation |
| `SLEEP_BETWEEN_DOWNLOADS` | (3, 6) | Random delay range in seconds. **Do not reduce below 3s** to avoid rate limiting |
| `AUDIO_QUALITY` | "0" | Best quality (0 = best, 10 = worst) |
| `EMBED_METADATA` | True | Embed ID3v2.3 tags (artist, album, year, etc.) from Spotify data |
| `EMBED_ALBUM_ART` | True | Download and embed album artwork from Spotify image URLs |

> **Note**: The sleep delays are intentionally set to be respectful of YouTube's servers. Reducing them may result in IP blocks or CAPTCHA challenges.

### Output

- **Downloads:** `downloads/Artist/Album/Track.mp3`
- **Progress CSV:** `data/spotify_playlists/main/liked_downloaded.csv`

### PO Token (If YouTube blocks downloads)

If you encounter HTTP 403 errors, you may need to provide a PO Token. See `doc/PO Token Guide.md` for instructions.

---

## Data

All data files are stored in the `data/` directory and are excluded from git to protect privacy.

---

## Metadata Embedding

The download script embeds rich metadata into MP3 files using the **ID3v2.3** standard, which is the most portable format across all music players and operating systems.

**Embedded tags include:**
- Title, Artist, Album Artist (separate fields for compilations)
- Album, Year, Track Number, Disc Number
- Genre (first genre from Spotify's list)
- ISRC (International Standard Recording Code)
- Label, Copyright
- Album Artwork (downloaded from Spotify CDN)

This allows you to rebuild your music library with full metadata from your Spotify data.

---

## Contributing

Contributions are welcome! Please ensure any changes:
1. Maintain or increase the default rate limiting delays
2. Do not add features that could enable mass scraping or abuse
3. Include appropriate error handling

---

## License

MIT License - This software is provided "as is", without warranty of any kind. Use at your own risk.

