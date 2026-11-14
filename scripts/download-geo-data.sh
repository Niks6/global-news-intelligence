#!/bin/bash
# Download Natural Earth geographic boundary data (public domain).
# Countries GeoJSON + Admin-1 (state/province) boundaries.
# Source: https://www.naturalearthdata.com/ — Public Domain license.

set -e
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
GEO_DIR="$PROJECT_DIR/data/geo"

mkdir -p "$GEO_DIR"

echo "Downloading Natural Earth countries (110m)..."
curl -L -o "$GEO_DIR/countries.geojson" \
  "https://raw.githubusercontent.com/nvkelso/natural-earth-vector/master/geojson/ne_110m_admin_0_countries.geojson"

echo "Downloading Natural Earth admin-1 states/provinces (10m)..."
curl -L -o "$GEO_DIR/admin1.geojson" \
  "https://raw.githubusercontent.com/nvkelso/natural-earth-vector/master/geojson/ne_10m_admin_1_states_provinces.geojson"

echo ""
echo "Downloaded to $GEO_DIR:"
ls -lh "$GEO_DIR"
echo ""
echo "License: Public Domain (Natural Earth)"
echo "Done."

