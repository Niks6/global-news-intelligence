"""Test all API endpoints."""
import urllib.request, json

endpoints = [
    ('/api/health', 'Health'),
    ('/api/health/ready', 'Readiness'),
    ('/api/health/pipeline', 'Pipeline Health'),
    ('/api/overview?from_dt=2026-11-17T00:00:00&to_dt=2026-11-24T00:00:00', 'Overview'),
    ('/api/events?page=1&page_size=10', 'Events'),
    ('/api/topics?from_dt=2026-11-17T00:00:00&to_dt=2026-11-24T00:00:00', 'Topics'),
    ('/api/countries', 'Countries'),
    ('/api/countries/US/overview?from_dt=2026-11-17T00:00:00&to_dt=2026-11-24T00:00:00', 'US Overview'),
    ('/api/compare/countries?codes=US,UK,FR&from_dt=2026-11-17T00:00:00&to_dt=2026-11-24T00:00:00', 'Compare'),
    ('/api/analytics/categories?from_dt=2026-11-17T00:00:00&to_dt=2026-11-24T00:00:00', 'Categories'),
    ('/api/analytics/tone?from_dt=2026-11-17T00:00:00&to_dt=2026-11-24T00:00:00', 'Tone'),
    ('/api/analytics/cooperation-conflict?from_dt=2026-11-17T00:00:00&to_dt=2026-11-24T00:00:00', 'Coop/Conflict'),
    ('/api/analytics/anomalies?from_dt=2026-11-17T00:00:00&to_dt=2026-11-24T00:00:00', 'Anomalies'),
    ('/api/map/events?from_dt=2026-11-17T00:00:00&to_dt=2026-11-24T00:00:00', 'Map Events'),
    ('/api/map/countries?from_dt=2026-11-17T00:00:00&to_dt=2026-11-24T00:00:00', 'Map Countries'),
    ('/api/search?q=government&from_dt=2026-11-17T00:00:00&to_dt=2026-11-24T00:00:00', 'Search'),
    ('/api/topics/CLIMATE_CHANGE/trend?from_dt=2026-11-17T00:00:00&to_dt=2026-11-24T00:00:00', 'Topic Trend'),
    ('/api/topics/CLIMATE_CHANGE/countries?from_dt=2026-11-17T00:00:00&to_dt=2026-11-24T00:00:00', 'Topic Countries'),
]

ok = fail = 0
for path, name in endpoints:
    try:
        resp = urllib.request.urlopen("http://localhost:8000" + path)
        data = json.loads(resp.read())
        has_data = any(
            (isinstance(data.get(k), list) and len(data[k]) > 0) or
            (isinstance(data.get(k), (int, float)) and data[k] > 0) or
            (isinstance(data.get(k), str) and data[k]) or
            (isinstance(data.get(k), bool))
            for k in data
        )
        ok += 1
        tag = "WITH_DATA" if has_data else "EMPTY"
        print("  PASS  %-20s %s" % (name, tag))
    except Exception as e:
        fail += 1
        print("  FAIL  %-20s %s" % (name, e))

print("\n=== %d/%d endpoints passed ===" % (ok, ok + fail))
