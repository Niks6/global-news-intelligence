# Geographic Data Documentation

This document describes all geographic data sources, processing strategies, and quality codes used by the Global News Intelligence Platform.

---

## 1. Data Sources

### 1.1 Natural Earth (Country and Region Boundaries)

**Source:** [Natural Earth](https://www.naturalearthdata.com/)  
**License:** Public Domain — no attribution required, but we credit the authors.

> "Natural Earth is a public domain map dataset available at 1:10m, 1:50m, and 1:110m map scales. Feature data is physically drawn by hand from trusted sources. Natural Earth was built over a period of two years by many volunteers in collaboration with NACIS (North American Cartographic Information Society)."

We use two Natural Earth vector datasets:

| Dataset | File | Usage |
|---|---|---|
| Admin 0 Countries (110m) | `ne_110m_admin_0_countries.geojson` | Country polygons for world map |
| Admin 1 States/Provinces (10m) | `ne_10m_admin_1_states_provinces.geojson` | Sub-national boundaries for region drill-down |

**GitHub source:** [nvkelso/natural-earth-vector](https://github.com/nvkelso/natural-earth-vector)

The GeoJSON files are stored locally at:
- `data/geo/countries.geojson`
- `data/geo/admin1.geojson`

Download them with the provided scripts (see [Section 5](#5-downloading-geographic-data)).

---

### 1.2 GDELT Event Coordinates

GDELT Events include geographic coordinates derived from the event location encoding. The relevant fields are:

| Field | Description |
|---|---|
| `ActionGeo_Lat` | Latitude of the event action location |
| `ActionGeo_Long` | Longitude of the event action location |
| `ActionGeo_Type` | Geographic resolution type code (1–5, see below) |
| `ActionGeo_FullName` | Human-readable location name |
| `ActionGeo_CountryCode` | FIPS country code of the action location |
| `ActionGeo_ADM1Code` | ADM1 (state/province) code |
| `Actor1Geo_Lat` | Latitude of Actor 1's location |
| `Actor1Geo_Long` | Longitude of Actor 1's location |
| `Actor2Geo_Lat` | Latitude of Actor 2's location |
| `Actor2Geo_Long` | Longitude of Actor 2's location |

#### GDELT GeoType Values

The `ActionGeo_Type` (and `Actor1Geo_Type`, `Actor2Geo_Type`) field encodes geographic resolution quality:

| GeoType Code | Meaning | Approximate Precision |
|---|---|---|
| `1` | Country | Country centroid — lowest resolution |
| `2` | US State | US state centroid |
| `3` | US City | US city/place — highest US resolution |
| `4` | World City | Non-US city/place — highest non-US resolution |
| `5` | World Region | Supranational or broad region centroid |
| `0` / missing | Unknown | No geographic coding possible |

> **Important:** A GeoType of 1 means the coordinate is a country centroid, not a precise event location. Many GDELT events have GeoType 1 coordinates that plot all events to the same point in the center of a country. Do not interpret country-centroid events as city-precise locations.

---

## 2. Geographic Resolution Quality Codes

The platform assigns an internal quality code to each event's geographic data:

| Quality Code | Meaning | GDELT GeoType |
|---|---|---|
| `EXACT` | City-level precision | GeoType 3 or 4 |
| `APPROXIMATE` | State/region-level precision | GeoType 2 or 5 |
| `COUNTRY` | Country centroid only | GeoType 1 |
| `UNKNOWN` | No geographic data available | GeoType 0 or missing |

This quality code is attached to every event record during the normalization phase and is exposed in API responses so that the frontend can render appropriate visual cues (e.g., smaller/dimmer markers for `COUNTRY`-level events on the map).

---

## 3. How Coordinates Are Used for Map Display

The React frontend uses [Leaflet](https://leafletjs.com/) with the GeoJSON country boundaries to render:

1. **Choropleth layer** (`data/geo/countries.geojson`) — country polygons shaded by event density or tone.
2. **Event marker layer** — individual event points plotted at `(ActionGeo_Lat, ActionGeo_Long)`.
3. **Region boundaries** (`data/geo/admin1.geojson`) — state/province polygons shown when the user drills down into a country.

Markers are only plotted when coordinates are available (non-null, non-zero) and the quality code is not `UNKNOWN`. Events with `COUNTRY`-level quality use a different visual style to avoid misleading the user about precision.

The backend `/api/map/events` endpoint filters and returns events with valid coordinates. Each event in the response carries:
- `lat`, `lon` — the coordinates
- `geo_quality` — one of `EXACT`, `APPROXIMATE`, `COUNTRY`, `UNKNOWN`
- `geo_type` — raw GDELT GeoType integer

---

## 4. Reverse Geocoding Strategy

For events where GDELT provides only coordinates but no human-readable region name (or where the ADM1 code needs resolution), the platform uses reverse geocoding to look up:
- Country name (when FIPS country code is unavailable)
- State/province name from ADM1Code
- City/locality name when GeoType is 3 or 4

### 4.1 Geocoding Provider

**Provider:** [geopy](https://geopy.readthedocs.io/) with the [Nominatim](https://nominatim.org/) geocoder (OpenStreetMap data).

**Rate limit:** Nominatim enforces a maximum of **1 request per second** per user agent. The platform enforces this with a token-bucket rate limiter in `src/common/geocoder.py`.

```python
# Nominatim usage policy:
# - Maximum 1 request per second
# - Must set a unique User-Agent header
# - Heavy usage requires self-hosting the Nominatim server
```

**User-Agent:** Set via `NOMINATIM_USER_AGENT` in `.env`. Default: `global-news-intelligence/1.0`.

### 4.2 Geocode Cache

All geocoding results are cached in a local SQLite database to avoid repeated API calls and to stay within rate limits.

**Cache location:** `data/cache/geocode_cache.db`

**Schema:**
```sql
CREATE TABLE geocode_cache (
    lat      REAL NOT NULL,
    lon      REAL NOT NULL,
    result   TEXT,          -- JSON-encoded geopy Location object
    cached_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (lat, lon)
);
```

Cache keys are `(lat, lon)` pairs rounded to 4 decimal places (~11 m precision) to maximize cache hits for coordinates that are functionally identical.

### 4.3 Reverse Geocoding in the Pipeline

Reverse geocoding is performed **offline / batch**, not on the hot path of ingestion. The `src/processing/geocoding_enrichment.py` script runs periodically against events that have coordinates but no resolved region name.

For the ingestion pipeline's hot path, the platform uses only the GDELT-provided fields (`ActionGeo_FullName`, `ActionGeo_ADM1Code`, `ActionGeo_CountryCode`) without additional geocoding API calls.

---

## 5. Downloading Geographic Data

### 5.1 Shell (Linux/macOS/WSL)

```bash
bash scripts/download-geo-data.sh
```

### 5.2 Windows (Command Prompt / PowerShell)

```bat
scripts\download-geo-data.bat
```

### 5.3 Manual Download URLs

| File | URL |
|---|---|
| `data/geo/countries.geojson` | https://raw.githubusercontent.com/nvkelso/natural-earth-vector/master/geojson/ne_110m_admin_0_countries.geojson |
| `data/geo/admin1.geojson` | https://raw.githubusercontent.com/nvkelso/natural-earth-vector/master/geojson/ne_10m_admin_1_states_provinces.geojson |

---

## 6. Important Limitations

- **Country actor ≠ event location.** GDELT's `Actor1CountryCode` / `Actor2CountryCode` describe the nationality of actors, not where the event physically occurred. Use `ActionGeo_CountryCode` for geographic filtering.
- **Country-centroid events are not geographically precise.** Events with GeoType 1 all share the same coordinate (the centroid of their country). Do not use them for city- or region-level analysis.
- **Nominatim rate limits** prevent bulk geocoding in real-time. All geocoding is done offline and cached.
- **GeoJSON file sizes:** `ne_10m_admin_1_states_provinces.geojson` is approximately 20 MB. The `ne_110m_admin_0_countries.geojson` is approximately 700 KB. Load the country file at startup; lazy-load the admin1 file on demand.

See [LIMITATIONS.md](LIMITATIONS.md) for a full discussion of data quality issues.
