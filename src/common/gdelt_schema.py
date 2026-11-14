"""
GDELT column schemas: exact column names and types for Events, Mentions, and GKG datasets.
Validated against the official GDELT 2.0 codebook.
Reference: https://data.gdeltproject.org/documentation/GDELT-Event_Codebook-V2.0.pdf
"""
from __future__ import annotations
from typing import Optional

# ---------------------------------------------------------------------------
# GDELT Events: 61 columns, TAB-separated
# ---------------------------------------------------------------------------
EVENTS_COLUMNS = [
    "GlobalEventID",         # 0  INT  — globally unique event ID (use for deduplication)
    "SQLDATE",               # 1  INT  — date YYYYMMDD
    "MonthYear",             # 2  INT  — month YYYYMM
    "Year",                  # 3  INT  — year YYYY
    "FractionDate",          # 4  FLOAT— fractional year (e.g. 2026.853)
    "Actor1Code",            # 5  STR  — full CAMEO actor1 code
    "Actor1Name",            # 6  STR  — actor1 name string
    "Actor1CountryCode",     # 7  STR  — actor1 3-letter FIPS country
    "Actor1KnownGroupCode",  # 8  STR
    "Actor1EthnicCode",      # 9  STR
    "Actor1Religion1Code",   # 10 STR
    "Actor1Religion2Code",   # 11 STR
    "Actor1Type1Code",       # 12 STR
    "Actor1Type2Code",       # 13 STR
    "Actor1Type3Code",       # 14 STR
    "Actor2Code",            # 15 STR
    "Actor2Name",            # 16 STR
    "Actor2CountryCode",     # 17 STR  — actor2 3-letter FIPS country
    "Actor2KnownGroupCode",  # 18 STR
    "Actor2EthnicCode",      # 19 STR
    "Actor2Religion1Code",   # 20 STR
    "Actor2Religion2Code",   # 21 STR
    "Actor2Type1Code",       # 22 STR
    "Actor2Type2Code",       # 23 STR
    "Actor2Type3Code",       # 24 STR
    "IsRootEvent",           # 25 INT  — 1 if root event of cluster
    "EventCode",             # 26 STR  — CAMEO event code (3-4 digits)
    "EventBaseCode",         # 27 STR  — CAMEO base code (3 digits)
    "EventRootCode",         # 28 STR  — CAMEO root code (2 digits, 01-20)
    "QuadClass",             # 29 INT  — 1=verbal coop,2=material coop,3=verbal conf,4=material conf
    "GoldsteinScale",        # 30 FLOAT— -10 (most conflict) to +10 (most cooperative)
    "NumMentions",           # 31 INT
    "NumSources",            # 32 INT
    "NumArticles",           # 33 INT
    "AvgTone",               # 34 FLOAT— average tone of articles (-100 to +100)
    "Actor1Geo_Type",        # 35 INT  — 1=country,2=US state,3=US city,4=world city,5=world region
    "Actor1Geo_FullName",    # 36 STR
    "Actor1Geo_CountryCode", # 37 STR
    "Actor1Geo_ADM1Code",    # 38 STR  — FIPS state/province code
    "Actor1Geo_ADM2Code",    # 39 STR
    "Actor1Geo_Lat",         # 40 FLOAT
    "Actor1Geo_Long",        # 41 FLOAT
    "Actor1Geo_FeatureID",   # 42 STR
    "Actor2Geo_Type",        # 43 INT
    "Actor2Geo_FullName",    # 44 STR
    "Actor2Geo_CountryCode", # 45 STR
    "Actor2Geo_ADM1Code",    # 46 STR
    "Actor2Geo_ADM2Code",    # 47 STR
    "Actor2Geo_Lat",         # 48 FLOAT
    "Actor2Geo_Long",        # 49 FLOAT
    "Actor2Geo_FeatureID",   # 50 STR
    "ActionGeo_Type",        # 51 INT  — geo type for the action location (most useful for map)
    "ActionGeo_FullName",    # 52 STR
    "ActionGeo_CountryCode", # 53 STR  — FIPS code for action location country
    "ActionGeo_ADM1Code",    # 54 STR
    "ActionGeo_ADM2Code",    # 55 STR
    "ActionGeo_Lat",         # 56 FLOAT— latitude of action location
    "ActionGeo_Long",        # 57 FLOAT— longitude of action location
    "ActionGeo_FeatureID",   # 58 STR
    "DATEADDED",             # 59 INT  — YYYYMMDDHHMMSS when record was added
    "SOURCEURL",             # 60 STR  — source article URL
]

EVENTS_NUMERIC_INT = {
    "GlobalEventID", "SQLDATE", "MonthYear", "Year", "IsRootEvent",
    "QuadClass", "NumMentions", "NumSources", "NumArticles", "DATEADDED",
    "Actor1Geo_Type", "Actor2Geo_Type", "ActionGeo_Type",
}

EVENTS_NUMERIC_FLOAT = {
    "FractionDate", "GoldsteinScale", "AvgTone",
    "Actor1Geo_Lat", "Actor1Geo_Long",
    "Actor2Geo_Lat", "Actor2Geo_Long",
    "ActionGeo_Lat", "ActionGeo_Long",
}

EVENTS_REQUIRED = {"GlobalEventID", "SQLDATE", "EventCode", "EventRootCode", "QuadClass"}

# ---------------------------------------------------------------------------
# GDELT Mentions: ~20 columns, TAB-separated
# ---------------------------------------------------------------------------
MENTIONS_COLUMNS = [
    "GlobalEventID",          # 0  INT  — links to Events
    "EventTimeDate",          # 1  INT  — YYYYMMDDHHMMSS event timestamp
    "MentionTimeDate",        # 2  INT  — YYYYMMDDHHMMSS mention timestamp
    "MentionType",            # 3  INT  — 1=WEB, 2=CITATIONONLY, 3=CORE, 4=DTIC, 5=JSTOR, 6=NONTEXTUALSOURCE
    "MentionSourceName",      # 4  STR  — source domain/name
    "MentionIdentifier",      # 5  STR  — URL or document ID (unique mention key)
    "SentenceID",             # 6  INT
    "Actor1CharOffset",       # 7  INT
    "Actor2CharOffset",       # 8  INT
    "ActionCharOffset",       # 9  INT
    "InRawText",              # 10 INT  — 1 if in raw text
    "Confidence",             # 11 INT  — confidence 0-100
    "MentionDocLen",          # 12 INT
    "MentionDocTone",         # 13 FLOAT— tone of this mention's article
    "MentionDocTranslationInfo", # 14 STR
    "Extras",                 # 15 STR
]

MENTIONS_NUMERIC_INT = {
    "GlobalEventID", "EventTimeDate", "MentionTimeDate", "MentionType",
    "SentenceID", "Actor1CharOffset", "Actor2CharOffset", "ActionCharOffset",
    "InRawText", "Confidence", "MentionDocLen",
}
MENTIONS_NUMERIC_FLOAT = {"MentionDocTone"}

# ---------------------------------------------------------------------------
# GDELT GKG: tab-separated columns, but many sub-fields use ; # , delimiters
# ---------------------------------------------------------------------------
GKG_COLUMNS = [
    "GKGRecordID",                 # 0  STR  — unique ID: YYYYMMDDHHMMSS-N
    "V2.1DATE",                    # 1  INT  — YYYYMMDDHHMMSS
    "V2SourceCollectionIdentifier",# 2  INT  — 1=WEB, 2=CITATIONONLY, 3=CORE, etc.
    "V2SourceCommonName",          # 3  STR  — source domain
    "V2DocumentIdentifier",        # 4  STR  — URL
    "V1Counts",                    # 5  STR  — semicolon-separated count references
    "V2.1Counts",                  # 6  STR
    "V1Themes",                    # 7  STR  — semicolon-separated GDELT themes
    "V2EnhancedThemes",            # 8  STR  — semicolon-separated enhanced themes (use this)
    "V1Locations",                 # 9  STR
    "V2EnhancedLocations",         # 10 STR  — semicolon-separated location records
    "V1Persons",                   # 11 STR  — semicolon-separated person names
    "V2EnhancedPersons",           # 12 STR
    "V1Organizations",             # 13 STR  — semicolon-separated org names
    "V2EnhancedOrganizations",     # 14 STR
    "V1.5Tone",                    # 15 STR  — comma: Tone,PositiveScore,NegativeScore,Polarity,ARD,SelfDens,WordCount
    "V2.1EnhancedDates",           # 16 STR
    "V2GCAM",                      # 17 STR  — GCAM dimensional scores (key=value pairs)
    "V2.1SharingImage",            # 18 STR
    "V2.1RelatedImages",           # 19 STR
    "V2.1SocialImageEmbeds",       # 20 STR
    "V2.1SocialVideoEmbeds",       # 21 STR
    "V2.1Quotations",              # 22 STR
    "V2.1AllNames",                # 23 STR
    "V2.1Amounts",                 # 24 STR
    "V2.1TranslationInfo",         # 25 STR
    "V2ExtrasXML",                 # 26 STR
]

# Sub-field parse helpers
GKG_TONE_FIELDS = ["Tone", "PositiveScore", "NegativeScore", "Polarity", "ARD", "SelfDensification", "WordCount"]

