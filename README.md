# De-Spotify

A collection of Python scripts to enrich Spotify playlist data with YouTube links and metadata from external sources (Songstats and Discogs).

## Main Scripts

- **songstats.py** - Main script to fetch YouTube links from Songstats using ISRC codes
- **discogs.py** - Main script to fetch YouTube links from Discogs API for albums
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

### Fetch YouTube links via Songstats
```bash
python code/songstats.py
```

### Fetch YouTube links via Discogs
```bash
python code/discogs.py
```

## Features

- Resumable processing - scripts can be stopped and restarted
- Progress tracking with status columns
- Rate limiting to respect API limits
- Automatic retries and error handling
- Multiple fallback strategies for finding YouTube links

## Data

All data files are stored in the `data/` directory and are excluded from git to protect privacy.
