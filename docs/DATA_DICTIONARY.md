# Data Dictionary

## GDELT Events (61 columns, TAB-separated)

| # | Column | Type | Description |
|---|--------|------|-------------|
| 0 | GlobalEventID | INT | Globally unique event identifier (deduplication key) |
| 1 | SQLDATE | INT | Event date as YYYYMMDD |
| 2 | MonthYear | INT | YYYYMM |
| 3 | Year | INT | YYYY |
| 4 | FractionDate | FLOAT | Fractional year (e.g. 2026.853) |
| 5 | Actor1Code | STR | Full CAMEO actor code |
| 6 | Actor1Name | STR | Actor name string |
| 7 | Actor1CountryCode | STR | 3-letter FIPS country code |
| 8-14 | Actor1 attributes | STR | KnownGroupCode, EthnicCode, Religion1/2, Type1/2/3 |
| 15 | Actor2Code | STR | Full CAMEO actor code |
| 16 | Actor2Name | STR | Actor name string |
| 17 | Actor2CountryCode | STR | 3-letter FIPS country code |
| 18-24 | Actor2 attributes | STR | Same as Actor1 attributes |
| 25 | IsRootEvent | INT | 1 if root event of story cluster |
| 26 | EventCode | STR | CAMEO event code (3-4 digits) |
| 27 | EventBaseCode | STR | CAMEO base code (3 digits) |
| 28 | EventRootCode | STR | CAMEO root code (2 digits, 01-20) |
| 29 | QuadClass | INT | 1=Verbal Coop, 2=Material Coop, 3=Verbal Conflict, 4=Material Conflict |
| 30 | GoldsteinScale | FLOAT | -10 (conflict) to +10 (cooperative) |
| 31 | NumMentions | INT | Number of mentions across all sources |
| 32 | NumSources | INT | Number of distinct source documents |
| 33 | NumArticles | INT | Number of distinct articles |
| 34 | AvgTone | FLOAT | Average reporting tone (-100 to +100) |
| 35-42 | Actor1Geo_* | Mixed | Actor1 geographic info: Type, FullName, CountryCode, ADM1, ADM2, Lat, Long, FeatureID |
| 43-50 | Actor2Geo_* | Mixed | Actor2 geographic info (same fields) |
| 51-58 | ActionGeo_* | Mixed | Action location: Type, FullName, CountryCode, ADM1, ADM2, Lat, Long, FeatureID |
| 59 | DATEADDED | INT | When record was added: YYYYMMDDHHMMSS |
| 60 | SOURCEURL | STR | Source article URL |

### GeoType Values
- 1 = Country, 2 = US State, 3 = US City, 4 = World City, 5 = World State/Region

### Important Note
**Actor1CountryCode is NOT necessarily where the event happened.** It is the country affiliation of Actor 1. Use `ActionGeo_CountryCode` for the event's physical location.

## GDELT Mentions (16 columns, TAB-separated)

| # | Column | Type | Description |
|---|--------|------|-------------|
| 0 | GlobalEventID | INT | Links to Events table |
| 1 | EventTimeDate | INT | YYYYMMDDHHMMSS |
| 2 | MentionTimeDate | INT | When this mention was published |
| 3 | MentionType | INT | 1=WEB, 2=CITATIONONLY, 3=CORE, etc. |
| 4 | MentionSourceName | STR | Source domain |
| 5 | MentionIdentifier | STR | URL or document ID |
| 6-12 | Offsets/Confidence | INT | SentenceID, char offsets, confidence |
| 13 | MentionDocTone | FLOAT | Tone of this specific article |
| 14-15 | Translation/Extras | STR | Metadata |

## GDELT GKG (27 columns, TAB-separated with complex sub-fields)

| # | Column | Type | Sub-delimiter | Description |
|---|--------|------|---------------|-------------|
| 0 | GKGRecordID | STR | — | Unique ID: YYYYMMDDHHMMSS-N |
| 4 | V2DocumentIdentifier | STR | — | Article URL |
| 7 | V1Themes | STR | `;` | Semicolon-separated theme list |
| 8 | V2EnhancedThemes | STR | `;` then `,` | Enhanced themes with offsets |
| 11 | V1Persons | STR | `;` | Person names |
| 13 | V1Organizations | STR | `;` | Organization names |
| 15 | V1.5Tone | STR | `,` | Tone,PosScore,NegScore,Polarity,ARD,SelfDens,WordCount |

## Pydantic Models (Python)

See `src/common/schemas.py` for complete model definitions:
- `GDELTEvent` — Raw 61-field event
- `GDELTMention` — Raw mention
- `GDELTGKGRecord` — Parsed GKG with exploded themes/persons/orgs
- `NormalizedEvent` — Enriched event with derived fields
- `CountryMetric`, `TopicMetric`, `CategoryMetric`, `AnomalyMetric` — Aggregate models
- `IngestionMetadata` — File processing tracking

## HBase Tables

See `docs/HDFS_HBASE_DESIGN.md` for complete HBase table schemas.

## CAMEO Event Taxonomy

See `src/common/cameo_mapping.py` for:
- Root codes 01-20 mapped to category names
- Cooperation (01-09) vs Conflict (10-20) classification
- GoldsteinScale interpretation thresholds

