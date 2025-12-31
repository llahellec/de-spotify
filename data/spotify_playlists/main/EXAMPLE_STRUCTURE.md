# Expected CSV Structure

Place your Spotify export CSV file here and rename it to `liked.csv` (or `Liked_Songs.csv`).

## Required Columns

The scripts expect these columns from a Spotify playlist export:

| Column | Description | Example |
|--------|-------------|---------|
| `track_uri` | Spotify track URI | `spotify:track:4iV5W9uYEdYUVa79Axb7Rh` |
| `track_name` | Song title | `My Kind of Woman` |
| `artist_name(s)` | Artist(s), comma-separated | `Daft Punk, Pharrell Williams` |
| `album_name` | Album title | `Random Access Memories` |
| `album_release_date` | Release date | `2013-05-17` |
| `album_image_url` | Spotify CDN image URL | `https://i.scdn.co/image/ab67616d...` |
| `track_number` | Track position on album | `8` |
| `disc_number` | Disc number | `1` |
| `track_duration(ms)` | Duration in milliseconds | `369000` |
| `isrc` | International Standard Recording Code | `USQX91300108` |
| `artist_genres` | Genre tags | `electronic, french house` |
| `label` | Record label | `Columbia` |
| `copyrights` | Copyright info | `(C) 2013 Columbia Records` |

## How to Export from Spotify

1. Use a tool like [Exportify](https://exportify.net/) to export your playlists
2. Or use the Spotify Web API to fetch your library data
3. Save the CSV file in this folder as `liked.csv`

## Workflow

1. `liked.csv` - Your original Spotify export (input)
2. `liked_yt_songstats.csv` - After running songstats.py (adds YouTube URLs)
3. `liked_yt_discogs.csv` - After running discogs.py (adds YouTube URLs)
4. `liked_master.csv` - After running merge_yt_urls.py (merged URLs)
5. `liked_downloaded.csv` - After running yt_download.py (tracks download status)
