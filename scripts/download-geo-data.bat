@echo off
REM Download geographic data for the Global News Intelligence platform
REM Run this once before starting the platform

set DATA_DIR=%~dp0..\data\geo

echo ====================================
echo  Geographic Data Downloader (Windows)
echo ====================================

if not exist "%DATA_DIR%" mkdir "%DATA_DIR%"

echo.
echo [1/3] Downloading FIPS country codes mapping...
curl -L -o "%DATA_DIR%\FIPS_country_codes.csv" ^
  "https://raw.githubusercontent.com/datasets/fips-10-4/master/data/data.csv"
if %ERRORLEVEL% NEQ 0 (
    echo WARNING: Failed to download FIPS mapping. Using built-in mapping.
) else (
    echo Done.
)

echo.
echo [2/3] Downloading GeoJSON country boundaries (simplified)...
curl -L -o "%DATA_DIR%\countries.geojson" ^
  "https://raw.githubusercontent.com/datasets/geo-countries/master/data/countries.geojson"
if %ERRORLEVEL% NEQ 0 (
    echo WARNING: Failed to download GeoJSON. Map visualization may not work.
) else (
    echo Done.
)

echo.
echo [3/3] Downloading country centroids for map markers...
curl -L -o "%DATA_DIR%\country_centroids.csv" ^
  "https://raw.githubusercontent.com/google/dspl/master/samples/google/canonical/countries.csv"
if %ERRORLEVEL% NEQ 0 (
    echo WARNING: Failed to download centroids. Map pins may not render.
) else (
    echo Done.
)

echo.
echo ====================================
echo  Download complete!
echo  Files saved to: %DATA_DIR%
echo ====================================
dir "%DATA_DIR%"
pause
