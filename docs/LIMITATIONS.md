# Known Limitations

## GDELT Data Quality

1. **Coordinates are approximate.** GDELT GeoType values: 1=country centroid, 2=US state centroid, 3=US city, 4=world city, 5=world state/region. Only types 3-4 provide city-level precision. Many events have GeoType=1, meaning the coordinates are the geographic center of a country — not where the event actually happened.

2. **Actor country ≠ event location.** `Actor1CountryCode` is the country affiliation of Actor 1 (e.g., "the US President" → US). `ActionGeo_CountryCode` is where the event physically occurred. The dashboard uses `ActionGeo_CountryCode` for country-level analysis and map display, and `Actor1CountryCode` only for actor analysis.

3. **Missing data.** Some records lack latitude/longitude, actor names, or source URLs. The dashboard shows a "no coordinates" count on the map and uses "—" for missing values.

4. **Duplicate events.** GDELT may publish the same event across multiple update files. We deduplicate by `GlobalEventID` in the checkpoint DB and in Spark streaming (via `dropDuplicates` with 1-hour watermark).

## Tone / Sentiment

5. **GDELT tone is NOT ground-truth sentiment.** `AvgTone` measures the average tone of *news articles reporting* on an event — the textual positivity/negativity of the language used. It does NOT measure: public opinion, factual severity of the event, or real-world sentiment. A disaster might have positive-tone articles if they focus on rescue efforts. The dashboard displays a disclaimer on all tone/sentiment views.

## Topic Extraction

6. **Topics are keyword-based, not ML-based.** Topics come from GDELT's GKG V2EnhancedThemes — a dictionary-based extraction, not a trained NLP model. High-frequency generic themes (e.g., "GENERAL_GOVERNMENT") are filtered out by prefix, but some noise remains. The system normalizes themes (lowercase, strip prefixes) but does not perform true semantic topic modeling.

## Anomaly Detection

7. **Z-score anomaly detection is simple.** The system compares a 2-hour window against a 24-hour baseline using a rolling z-score. This is effective for sudden spikes but may produce false positives during periodic patterns (e.g., daily news cycles). A minimum volume threshold of 10 events reduces noise from tiny baselines. Production systems would use more sophisticated methods (EWMA, seasonal decomposition).

## Cluster / Infrastructure

8. **3-PC academic cluster is NOT production-grade.** With only 3 machines and ~8 GB RAM each:
   - **No high availability.** Single Kafka broker, single HDFS NameNode, single HBase Master. Any single point of failure brings down that service.
   - **HDFS replication factor 2** tolerates 1 DataNode failure. If 2 nodes fail simultaneously, data may be lost.
   - **No automatic failover.** Manual restart required after any node crash.

9. **Wi-Fi networking limitations.** Wi-Fi has higher latency, lower bandwidth, and less reliability than Ethernet. Streaming throughput may be limited. Client isolation on the router may block inter-machine traffic.

10. **WSL2 networking is unreliable for clusters.** WSL2 uses NAT networking with a virtual IP that changes on restart. Port forwarding is required but fragile. Native Ubuntu is strongly recommended for cluster nodes.

## Region-Level Data

11. **State/province data quality varies.** Region-level drill-down depends on GDELT's `ActionGeo_ADM1Code` field, which may be missing or imprecise. The system shows geographic resolution quality (EXACT/APPROXIMATE/COUNTRY/UNKNOWN) and does not fabricate state assignments from weak coordinates.

## Rate Limits

12. **Nominatim geocoding: 1 request/second.** Reverse geocoding for coordinates uses OpenStreetMap Nominatim, which enforces strict rate limits. Results are cached in SQLite to avoid re-querying. Large-scale geocoding is slow.

13. **GDELT DOC API: 1 request/minute.** The DOC API is supplementary, not the primary ingestion route. Bulk data comes from raw file downloads, which have no explicit rate limit.

## Scale

14. **Not designed for full GDELT backfill.** GDELT publishes ~60,000 events per 15-minute update. A full historical backfill would require significant storage and processing capacity beyond 3 academic PCs. The system is designed for real-time streaming of current data.

