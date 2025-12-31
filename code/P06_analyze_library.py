"""
Comprehensive Music Library Analysis

Creates a beautiful HTML report with interactive charts analyzing:
- Audio features (danceability, energy, valence, etc.)
- Genre distribution
- Temporal trends (by year/decade)
- Artist and album statistics
- Popularity analysis
- Audio feature correlations

Generates: music_library_analysis.html
"""

import csv
import json
from pathlib import Path
from collections import defaultdict, Counter
from datetime import datetime

# Paths
folder_path = Path(__file__).resolve().parents[1]
master_csv = folder_path / "data/spotify_playlists/main/liked_master.csv"
output_html = folder_path / "music_library_analysis.html"

print("=" * 70)
print("MUSIC LIBRARY ANALYSIS")
print("=" * 70)
print(f"\nLoading data from: {master_csv.name}")

# Load and process data
tracks = []
with open(master_csv, 'r', encoding='utf-8', newline='') as f:
    reader = csv.DictReader(f)
    for row in reader:
        tracks.append(row)

print(f"Loaded {len(tracks):,} tracks")
print("\nAnalyzing data...")

# ============================================================================
# DATA PROCESSING
# ============================================================================

# Audio features
audio_features = {
    'danceability': [],
    'energy': [],
    'speechiness': [],
    'acousticness': [],
    'instrumentalness': [],
    'liveness': [],
    'valence': [],
    'tempo': [],
    'loudness': []
}

# Metadata
years = []
decades = defaultdict(int)
artists = Counter()
albums = Counter()
genres = Counter()
explicit_count = 0
popularity_dist = defaultdict(int)
duration_minutes = []

# YouTube coverage
has_url = 0
url_sources = Counter()

# Process each track
for track in tracks:
    # Audio features (convert from 0-1000 scale to 0-1 for some)
    try:
        audio_features['danceability'].append(float(track.get('danceability', 0)) / 1000)
    except:
        pass
    try:
        audio_features['energy'].append(float(track.get('energy', 0)) / 1000)
    except:
        pass
    try:
        audio_features['speechiness'].append(float(track.get('speechiness', 0)))
    except:
        pass
    try:
        audio_features['acousticness'].append(float(track.get('acousticness', 0)))
    except:
        pass
    try:
        audio_features['instrumentalness'].append(float(track.get('instrumentalness', 0)))
    except:
        pass
    try:
        audio_features['liveness'].append(float(track.get('liveness', 0)) / 1000)
    except:
        pass
    try:
        audio_features['valence'].append(float(track.get('valence', 0)) / 1000)
    except:
        pass
    try:
        audio_features['tempo'].append(float(track.get('tempo', 0)) / 1000)
    except:
        pass
    try:
        audio_features['loudness'].append(float(track.get('loudness', 0)) / 1000)
    except:
        pass

    # Year and decade
    release_date = track.get('album_release_date', '')
    if len(release_date) >= 4:
        try:
            year = int(release_date[:4])
            years.append(year)
            decade = (year // 10) * 10
            decades[decade] += 1
        except:
            pass

    # Artists
    artist_name = track.get('artist_name(s)', '').strip()
    if artist_name:
        # Handle multiple artists
        for artist in artist_name.split(','):
            artists[artist.strip()[:50]] += 1

    # Albums
    album_name = track.get('album_name', '').strip()
    if album_name:
        albums[album_name[:50]] += 1

    # Genres
    genre_str = track.get('artist_genres', '').strip()
    if genre_str:
        for genre in genre_str.split(','):
            genre = genre.strip()
            if genre:
                genres[genre] += 1

    # Explicit
    if track.get('explicit', '').lower() in ['true', '1']:
        explicit_count += 1

    # Popularity
    try:
        pop = int(track.get('popularity', 0))
        pop_bucket = (pop // 10) * 10
        popularity_dist[pop_bucket] += 1
    except:
        pass

    # Duration
    try:
        duration_ms = int(track.get('track_duration(ms)', 0))
        duration_minutes.append(duration_ms / 60000)
    except:
        pass

    # URL coverage
    yt_url = track.get('yt_url', '').strip()
    if yt_url:
        has_url += 1
        origin = track.get('yt_url_origin', 'unknown').strip()
        url_sources[origin if origin else 'songstats'] += 1

# Calculate statistics
def get_stats(values):
    if not values:
        return {'min': 0, 'max': 0, 'avg': 0, 'median': 0}
    sorted_vals = sorted(values)
    return {
        'min': sorted_vals[0],
        'max': sorted_vals[-1],
        'avg': sum(values) / len(values),
        'median': sorted_vals[len(sorted_vals) // 2]
    }

print("Generating visualizations...")

# ============================================================================
# HTML GENERATION WITH CHART.JS
# ============================================================================

html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Music Library Analysis</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: #333;
            padding: 20px;
        }}
        .container {{
            max-width: 1400px;
            margin: 0 auto;
        }}
        .header {{
            background: white;
            border-radius: 20px;
            padding: 40px;
            margin-bottom: 30px;
            box-shadow: 0 10px 40px rgba(0,0,0,0.1);
            text-align: center;
        }}
        h1 {{
            color: #667eea;
            font-size: 3em;
            margin-bottom: 10px;
        }}
        .subtitle {{
            color: #666;
            font-size: 1.2em;
        }}
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }}
        .stat-card {{
            background: white;
            border-radius: 15px;
            padding: 25px;
            box-shadow: 0 5px 20px rgba(0,0,0,0.1);
            transition: transform 0.3s ease;
        }}
        .stat-card:hover {{
            transform: translateY(-5px);
        }}
        .stat-value {{
            font-size: 2.5em;
            font-weight: bold;
            color: #667eea;
            margin: 10px 0;
        }}
        .stat-label {{
            color: #666;
            font-size: 1em;
            text-transform: uppercase;
            letter-spacing: 1px;
        }}
        .chart-container {{
            background: white;
            border-radius: 15px;
            padding: 30px;
            margin-bottom: 30px;
            box-shadow: 0 5px 20px rgba(0,0,0,0.1);
        }}
        .chart-title {{
            font-size: 1.5em;
            font-weight: bold;
            color: #333;
            margin-bottom: 20px;
            text-align: center;
        }}
        .chart-wrapper {{
            position: relative;
            height: 400px;
        }}
        .chart-wrapper.large {{
            height: 500px;
        }}
        .grid-2 {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(500px, 1fr));
            gap: 30px;
        }}
        .footer {{
            text-align: center;
            color: white;
            margin-top: 40px;
            padding: 20px;
        }}
        @media (max-width: 768px) {{
            .grid-2 {{
                grid-template-columns: 1fr;
            }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🎵 Music Library Analysis</h1>
            <p class="subtitle">Deep dive into your {len(tracks):,} tracks</p>
            <p class="subtitle">Generated on {datetime.now().strftime('%B %d, %Y at %H:%M')}</p>
        </div>

        <div class="stats-grid">
            <div class="stat-card">
                <div class="stat-label">Total Tracks</div>
                <div class="stat-value">{len(tracks):,}</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Unique Artists</div>
                <div class="stat-value">{len(artists):,}</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Unique Albums</div>
                <div class="stat-value">{len(albums):,}</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">YouTube Coverage</div>
                <div class="stat-value">{has_url/len(tracks)*100:.1f}%</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Avg Track Length</div>
                <div class="stat-value">{sum(duration_minutes)/len(duration_minutes) if duration_minutes else 0:.1f} min</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Explicit Tracks</div>
                <div class="stat-value">{explicit_count/len(tracks)*100:.1f}%</div>
            </div>
        </div>

        <div class="chart-container">
            <div class="chart-title">📊 Music Collection Timeline</div>
            <div class="chart-wrapper large">
                <canvas id="decadeChart"></canvas>
            </div>
        </div>

        <div class="grid-2">
            <div class="chart-container">
                <div class="chart-title">🎸 Top 15 Artists</div>
                <div class="chart-wrapper">
                    <canvas id="artistChart"></canvas>
                </div>
            </div>
            <div class="chart-container">
                <div class="chart-title">💿 Top 15 Albums</div>
                <div class="chart-wrapper">
                    <canvas id="albumChart"></canvas>
                </div>
            </div>
        </div>

        <div class="chart-container">
            <div class="chart-title">🎭 Top 20 Genres</div>
            <div class="chart-wrapper">
                <canvas id="genreChart"></canvas>
            </div>
        </div>

        <div class="chart-container">
            <div class="chart-title">🎵 Audio Features Distribution</div>
            <div class="chart-wrapper large">
                <canvas id="audioFeaturesChart"></canvas>
            </div>
        </div>

        <div class="grid-2">
            <div class="chart-container">
                <div class="chart-title">⭐ Popularity Distribution</div>
                <div class="chart-wrapper">
                    <canvas id="popularityChart"></canvas>
                </div>
            </div>
            <div class="chart-container">
                <div class="chart-title">🔗 YouTube URL Sources</div>
                <div class="chart-wrapper">
                    <canvas id="urlSourceChart"></canvas>
                </div>
            </div>
        </div>

        <div class="chart-container">
            <div class="chart-title">⏱️ Track Duration Distribution</div>
            <div class="chart-wrapper">
                <canvas id="durationChart"></canvas>
            </div>
        </div>

        <div class="footer">
            <p>Generated by Music Library Analyzer</p>
            <p>Data from: {master_csv.name}</p>
        </div>
    </div>

    <script>
        // Chart.js default settings
        Chart.defaults.font.family = '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif';
        Chart.defaults.plugins.legend.display = true;
        Chart.defaults.plugins.legend.position = 'top';

        // Color palettes
        const colors = {{
            primary: ['#667eea', '#764ba2', '#f093fb', '#4facfe', '#43e97b', '#fa709a', '#fee140', '#30cfd0'],
            gradient: ['#667eea', '#5f72d8', '#5765c6', '#4f59b4', '#474ca2', '#3f4090'],
            warm: ['#fa709a', '#fee140', '#30cfd0', '#43e97b', '#f093fb', '#4facfe']
        }};

        // Decade Chart
        new Chart(document.getElementById('decadeChart'), {{
            type: 'bar',
            data: {{
                labels: {json.dumps([str(d) + 's' for d in sorted(decades.keys())])},
                datasets: [{{
                    label: 'Number of Tracks',
                    data: {json.dumps([decades[d] for d in sorted(decades.keys())])},
                    backgroundColor: 'rgba(102, 126, 234, 0.8)',
                    borderColor: 'rgba(102, 126, 234, 1)',
                    borderWidth: 2
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                plugins: {{
                    legend: {{ display: false }},
                    tooltip: {{
                        callbacks: {{
                            label: (context) => `Tracks: ${{context.parsed.y.toLocaleString()}}`
                        }}
                    }}
                }},
                scales: {{
                    y: {{
                        beginAtZero: true,
                        ticks: {{
                            callback: (value) => value.toLocaleString()
                        }}
                    }}
                }}
            }}
        }});

        // Top Artists Chart
        new Chart(document.getElementById('artistChart'), {{
            type: 'bar',
            data: {{
                labels: {json.dumps([artist for artist, _ in artists.most_common(15)])},
                datasets: [{{
                    label: 'Tracks',
                    data: {json.dumps([count for _, count in artists.most_common(15)])},
                    backgroundColor: colors.gradient,
                    borderWidth: 0
                }}]
            }},
            options: {{
                indexAxis: 'y',
                responsive: true,
                maintainAspectRatio: false,
                plugins: {{
                    legend: {{ display: false }}
                }},
                scales: {{
                    x: {{
                        beginAtZero: true
                    }}
                }}
            }}
        }});

        // Top Albums Chart
        new Chart(document.getElementById('albumChart'), {{
            type: 'bar',
            data: {{
                labels: {json.dumps([album for album, _ in albums.most_common(15)])},
                datasets: [{{
                    label: 'Tracks',
                    data: {json.dumps([count for _, count in albums.most_common(15)])},
                    backgroundColor: colors.warm,
                    borderWidth: 0
                }}]
            }},
            options: {{
                indexAxis: 'y',
                responsive: true,
                maintainAspectRatio: false,
                plugins: {{
                    legend: {{ display: false }}
                }},
                scales: {{
                    x: {{
                        beginAtZero: true
                    }}
                }}
            }}
        }});

        // Top Genres Chart
        new Chart(document.getElementById('genreChart'), {{
            type: 'bar',
            data: {{
                labels: {json.dumps([genre for genre, _ in genres.most_common(20)])},
                datasets: [{{
                    label: 'Tracks',
                    data: {json.dumps([count for _, count in genres.most_common(20)])},
                    backgroundColor: 'rgba(250, 112, 154, 0.8)',
                    borderColor: 'rgba(250, 112, 154, 1)',
                    borderWidth: 2
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                plugins: {{
                    legend: {{ display: false }}
                }},
                scales: {{
                    x: {{
                        ticks: {{
                            maxRotation: 45,
                            minRotation: 45
                        }}
                    }},
                    y: {{
                        beginAtZero: true
                    }}
                }}
            }}
        }});

        // Audio Features Radar Chart
        new Chart(document.getElementById('audioFeaturesChart'), {{
            type: 'radar',
            data: {{
                labels: ['Danceability', 'Energy', 'Speechiness', 'Acousticness', 'Instrumentalness', 'Liveness', 'Valence'],
                datasets: [{{
                    label: 'Average Values',
                    data: [
                        {sum(audio_features['danceability'])/len(audio_features['danceability']) if audio_features['danceability'] else 0},
                        {sum(audio_features['energy'])/len(audio_features['energy']) if audio_features['energy'] else 0},
                        {sum(audio_features['speechiness'])/len(audio_features['speechiness']) if audio_features['speechiness'] else 0},
                        {sum(audio_features['acousticness'])/len(audio_features['acousticness']) if audio_features['acousticness'] else 0},
                        {sum(audio_features['instrumentalness'])/len(audio_features['instrumentalness']) if audio_features['instrumentalness'] else 0},
                        {sum(audio_features['liveness'])/len(audio_features['liveness']) if audio_features['liveness'] else 0},
                        {sum(audio_features['valence'])/len(audio_features['valence']) if audio_features['valence'] else 0}
                    ],
                    backgroundColor: 'rgba(102, 126, 234, 0.2)',
                    borderColor: 'rgba(102, 126, 234, 1)',
                    borderWidth: 2,
                    pointBackgroundColor: 'rgba(102, 126, 234, 1)',
                    pointBorderColor: '#fff',
                    pointHoverBackgroundColor: '#fff',
                    pointHoverBorderColor: 'rgba(102, 126, 234, 1)'
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                scales: {{
                    r: {{
                        beginAtZero: true,
                        max: 1
                    }}
                }}
            }}
        }});

        // Popularity Distribution
        new Chart(document.getElementById('popularityChart'), {{
            type: 'line',
            data: {{
                labels: {json.dumps([str(i) + '-' + str(i+9) for i in sorted(popularity_dist.keys())])},
                datasets: [{{
                    label: 'Number of Tracks',
                    data: {json.dumps([popularity_dist[k] for k in sorted(popularity_dist.keys())])},
                    backgroundColor: 'rgba(67, 233, 123, 0.2)',
                    borderColor: 'rgba(67, 233, 123, 1)',
                    borderWidth: 3,
                    fill: true,
                    tension: 0.4
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                plugins: {{
                    legend: {{ display: false }}
                }},
                scales: {{
                    y: {{
                        beginAtZero: true
                    }}
                }}
            }}
        }});

        // URL Source Pie Chart
        new Chart(document.getElementById('urlSourceChart'), {{
            type: 'doughnut',
            data: {{
                labels: {json.dumps(list(url_sources.keys()))},
                datasets: [{{
                    data: {json.dumps(list(url_sources.values()))},
                    backgroundColor: colors.primary,
                    borderWidth: 2,
                    borderColor: '#fff'
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                plugins: {{
                    legend: {{
                        position: 'bottom'
                    }}
                }}
            }}
        }});

        // Duration Distribution
        const durationBuckets = {{
            '0-2 min': 0,
            '2-3 min': 0,
            '3-4 min': 0,
            '4-5 min': 0,
            '5-7 min': 0,
            '7+ min': 0
        }};
        {json.dumps(duration_minutes)}.forEach(d => {{
            if (d < 2) durationBuckets['0-2 min']++;
            else if (d < 3) durationBuckets['2-3 min']++;
            else if (d < 4) durationBuckets['3-4 min']++;
            else if (d < 5) durationBuckets['4-5 min']++;
            else if (d < 7) durationBuckets['5-7 min']++;
            else durationBuckets['7+ min']++;
        }});

        new Chart(document.getElementById('durationChart'), {{
            type: 'bar',
            data: {{
                labels: Object.keys(durationBuckets),
                datasets: [{{
                    label: 'Number of Tracks',
                    data: Object.values(durationBuckets),
                    backgroundColor: 'rgba(48, 207, 208, 0.8)',
                    borderColor: 'rgba(48, 207, 208, 1)',
                    borderWidth: 2
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                plugins: {{
                    legend: {{ display: false }}
                }},
                scales: {{
                    y: {{
                        beginAtZero: true
                    }}
                }}
            }}
        }});
    </script>
</body>
</html>
"""

# Write HTML file
with open(output_html, 'w', encoding='utf-8') as f:
    f.write(html_content)

print(f"\n✓ Analysis complete!")
print(f"✓ Report generated: {output_html}")
print(f"\nOpen {output_html.name} in your browser to view the interactive analysis!")
print("=" * 70)
