# Ukraine Frontline Tracker

Dual-source frontline comparison tool overlaying **DeepState UA** (Ukrainian OSINT) and **divgen.ru** (Russian OSINT) occupied territory polygons on the same map, with satellite fire detection, air raid alerts, sector activity ranking, and auto-generated daily briefings.

All territorial comparisons exclude Crimea by clipping DeepState to divgen's geographic footprint — giving an apples-to-apples mainland comparison.

## Quick Start

```bash
pip install -r requirements.txt
python server.py
# Open http://localhost:5001
```

First launch builds a weekly time series in the background (~90s for DeepState, ~5min for divgen). Subsequent launches are instant (all cached).

## What You See

**Always on the map:**
- **Red fill** — DeepState occupied territory (Ukrainian assessment)
- **Purple dashed outline** — divgen.ru frontline (Russian assessment)
- **Red/purple dashed zones** — disagreement areas where the two sources differ

**Right panel — paired metrics (excl. Crimea):**
```
              DeepState    Divgen     Gap
Occupied      88,089       95,246    +7,157 km²
% of Ukraine  15.3%        16.5%
Frontline     4,577 km
7d rate       +4.9 km²/d
```

**Daily change — dual source:**
```
              DS           Divgen
RU gains     +34.55       +47.78
UA recaptured −0.07        −0.35
Net          +34.48       +47.78
```

## Data Sources

| Source | Data | Auth | Update |
|--------|------|------|--------|
| [DeepState UA](https://deepstatemap.live/en) via [cyterat](https://github.com/cyterat/deepstate-map-data) | Occupied territory polygons | None | Daily 03:00 UTC |
| [divgen.ru](https://divgen.ru) | KML frontline (scraped, negative-space extraction) | Session cookie (auto) | Per-event |
| [NASA FIRMS](https://firms.modaps.eosdis.nasa.gov/) | VIIRS thermal hotspots (375m) | None | Every few hours |
| [sirens.in.ua](https://sirens.in.ua/) | Air raid alert status | None | Real-time |
| [Open-Meteo](https://open-meteo.com/) | Weather conditions | None | Hourly |
| [Natural Earth](https://www.naturalearthdata.com/) | Oblast boundaries | None | Static |

## Features

### Map Layers (toggleable)
- **Daily diff** — red = Russian gains, green = UA recaptures (Shapely geometric difference)
- **DS/Divgen disagreement zones** — where the two sources draw the line differently
- **Change heatmap** — union of all gains/losses over 7-90 day window
- **Ghost frontlines** — dashed lines showing where the front was 30/90/180 days ago
- **Salient detection** — algorithmically finds vulnerable bulges via morphological opening (5km erosion/dilation)
- **NASA FIRMS** — satellite-detected fires sized by radiative power (MW), 24h/48h/7d
- **Air raid alerts** — live siren status per oblast, auto-refreshing every 30s
- **Frontline weather** — current conditions at 8 frontline locations
- **City distance markers** — distance from frontline to key cities
- **Time-to-city projections** — at current rate, when does the frontline reach Kramatorsk?
- **Animated frontline pulse** — marching-ants on the contact line
- **Side-by-side comparison** — split-screen synced maps for two dates
- **Measurement tool** — click two points for distance in km
- **Terrain/hillshade overlay** — elevation layer for natural defensive lines

### Intelligence
- **Hottest Sectors** — ranks 10 frontline sectors by composite activity score (fire density × 0.5 + area change × 0.3 + frontline length × 0.2). Shows fires, km² change, and frontline km per sector with HIGH/MEDIUM/LOW rating.
- **Daily Briefing** — auto-generated text report covering territory snapshot, source comparison, daily change, nearest cities, fire activity, and air raid alerts. One-click copy to clipboard for reports/newsletters.

### Analytics
- **Oblast occupation** — bar chart (Luhansk 98.8%, Donetsk 78.5%, Zaporizhzhia 75.3%, Kherson 72.1%)
- **Dual-line area chart** — DS (red) vs Divgen (purple) with shaded disagreement gap
- **Rate-of-advance chart** — red bars = RU gains, green = UA recaptures (7d rolling)
- **DS−Divgen gap trend** — is the disagreement growing or shrinking?
- **Crimea auto-exclusion** — comparison clips DS to divgen's footprint, no hardcoded boxes

### Controls
- 3 basemaps: CARTO Dark, Esri Satellite, CARTO Light
- Timeline: 612 daily dates (2024-07-08 → today), 4 playback speeds
- URL state encoding (shareable links)
- CSV/JSON time series export
- War dashboard ticker (day count, area, frontline, weekly change)

### Keyboard Shortcuts

| Key | Action | Key | Action |
|-----|--------|-----|--------|
| `←` `→` | ±1 day | `H` | Heatmap |
| `Shift+←→` | ±7 days | `G` | Ghost frontlines |
| `Space` | Play/pause | `X` | Salients |
| `Home`/`End` | Jump start/end | `F` | NASA FIRMS |
| `S` | Satellite basemap | `A` | Air raid alerts |
| `C` | Side-by-side | `W` | Weather |
| `M` | Measure tool | `R` | Disagreement zones |
| `D` | Daily diff | | |

## Architecture

```
frontline-tracker/
├── server.py              # Flask server + 20 API endpoints
├── data_pipeline.py       # Data sources + analytics
│   ├── DeepStateSource    # cyterat GitHub archive (daily GeoJSON)
│   ├── DivgenSource       # divgen.ru KML scraping (negative-space extraction)
│   ├── FrontlineAnalytics # Area, length, diff, rates, oblasts, salients, projections
│   ├── BriefingGenerator  # Auto-generated daily text briefings
│   ├── HottestSectors     # Sector activity ranking (fires + movement)
│   ├── NASAFirms          # VIIRS fire hotspots (global CSV filtered to Ukraine)
│   ├── AirRaidAlerts      # sirens.in.ua live status
│   └── FrontlineWeather   # Open-Meteo conditions
├── requirements.txt       # flask, requests, shapely, pyproj, gunicorn
├── render.yaml            # Render.com deploy config
├── data/
│   ├── cache/             # ~70KB per date (DS) + ~100KB per date (divgen KML→GeoJSON)
│   ├── oblasts.geojson    # Ukraine admin boundaries (Natural Earth)
│   ├── available_dates.json
│   ├── time_series.json   # DS weekly area/frontline (cached)
│   └── divgen_time_series.json  # Divgen weekly area (cached)
└── templates/
    └── index.html         # Single-file Leaflet frontend (no build step)
```

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /api/snapshot/<d>` | **Batch** — all data for a date in one call |
| `GET /api/dates` | Available DeepState dates (YYYYMMDD) |
| `GET /api/date/<d>` | Full-fidelity occupied territory GeoJSON |
| `GET /api/stats/<d>` | Area km², frontline km, polygon count |
| `GET /api/diff/dual/<d1>/<d2>` | Both DS + Divgen territorial diffs |
| `GET /api/compare/<d>` | DS vs Divgen disagreement (clipped, excl Crimea) |
| `GET /api/divgen/date/<d>` | Divgen occupied territory GeoJSON |
| `GET /api/distances/<d>` | Distance from frontline to key cities |
| `GET /api/oblasts/<d>` | Oblast-level occupation % |
| `GET /api/projections/<d>` | Time-to-city estimates at current rate |
| `GET /api/salients/<d>` | Detected salients with vulnerability scores |
| `GET /api/ghosts/<d>` | Frontline boundaries 30/90/180d ago |
| `GET /api/heatmap/<d>?days=30` | Change heatmap (union of diffs) |
| `GET /api/sectors/<d>` | Hottest sectors ranked by activity |
| `GET /api/briefing/<d>` | Auto-generated daily text briefing |
| `GET /api/time-series/dual` | Both DS + Divgen weekly time series |
| `GET /api/fires?period=24h` | NASA FIRMS GeoJSON |
| `GET /api/alerts` | Air raid alert status |
| `GET /api/weather` | Frontline weather |
| `GET /api/export/csv` | Download time series CSV |
| `GET /api/export/json` | Download time series JSON |

## Key Insight

With Crimea excluded (auto-clipped via divgen's footprint), **divgen claims ~7,000 km² MORE mainland occupied territory than DeepState**. The Russian source is more aggressive about claiming gains than the Ukrainian source acknowledges. The daily diff confirms this: for the same week, divgen reports +48 km² Russian gains vs DeepState's +35 km².

## License

- DeepState data: [deepstatemap.live/license](https://deepstatemap.live/license-en.html) — free for non-commercial/volunteer/defense use
- divgen.ru: scraped KML, used with attribution per their terms
- NASA FIRMS: public domain (US government)
- Open-Meteo: [CC BY 4.0](https://open-meteo.com/en/terms)
