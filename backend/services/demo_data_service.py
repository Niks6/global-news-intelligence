"""
Demo data service — generates realistic sample data for the dashboard
when HBase / Kafka / Spark infrastructure is not running.

This service is activated automatically when HBase is unreachable.
It provides deterministic, realistic-looking data so the full frontend
can be verified end-to-end.
"""
from __future__ import annotations

import hashlib
import math
import random
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

# Seed for reproducibility
_RNG = random.Random(42)

# --- Reference data -----------------------------------------------------------

COUNTRIES = [
    ("US", "United States", 38.9, -77.0),
    ("UK", "United Kingdom", 51.5, -0.1),
    ("FR", "France", 48.9, 2.3),
    ("GM", "Germany", 52.5, 13.4),
    ("RS", "Russia", 55.8, 37.6),
    ("CH", "China", 39.9, 116.4),
    ("JA", "Japan", 35.7, 139.7),
    ("IN", "India", 28.6, 77.2),
    ("BR", "Brazil", -15.8, -47.9),
    ("AU", "Australia", -35.3, 149.1),
    ("CA", "Canada", 45.4, -75.7),
    ("SF", "South Africa", -33.9, 18.4),
    ("EG", "Egypt", 30.0, 31.2),
    ("TU", "Turkey", 39.9, 32.9),
    ("SA", "Saudi Arabia", 24.7, 46.7),
    ("NI", "Nigeria", 9.1, 7.5),
    ("IS", "Israel", 31.8, 35.2),
    ("KE", "Kenya", -1.3, 36.8),
    ("MX", "Mexico", 19.4, -99.1),
    ("ID", "Indonesia", -6.2, 106.8),
]

CAMEO_CATEGORIES = {
    "01": ("Make Public Statement", False),
    "02": ("Appeal", False),
    "03": ("Express Intent to Cooperate", False),
    "04": ("Consult", False),
    "05": ("Engage in Diplomatic Cooperation", False),
    "06": ("Engage in Material Cooperation", False),
    "07": ("Provide Aid", False),
    "08": ("Yield", False),
    "09": ("Investigate", False),
    "10": ("Demand", True),
    "11": ("Disapprove", True),
    "12": ("Reject", True),
    "13": ("Threaten", True),
    "14": ("Protest", True),
    "15": ("Exhibit Military Posture", True),
    "16": ("Reduce Relations", True),
    "17": ("Coerce", True),
    "18": ("Assault", True),
    "19": ("Fight", True),
    "20": ("Use Unconventional Mass Violence", True),
}

TOPICS = [
    "CLIMATE_CHANGE", "TRADE_AGREEMENT", "MILITARY_EXERCISE", "ELECTION",
    "DIPLOMATIC_MEETING", "ECONOMIC_SANCTIONS", "HUMANITARIAN_AID",
    "TERRORISM", "CYBERSECURITY", "ENERGY_POLICY", "MIGRATION",
    "HEALTHCARE", "TECHNOLOGY", "PROTESTS", "INFRASTRUCTURE",
]

QUAD_CLASS_NAMES = {
    1: "Verbal Cooperation",
    2: "Material Cooperation",
    3: "Verbal Conflict",
    4: "Material Conflict",
}


def _hash_seed(key: str) -> int:
    return int(hashlib.md5(key.encode()).hexdigest()[:8], 16)


def _date_range_days(from_dt: datetime, to_dt: datetime) -> List[str]:
    """Return list of YYYY-MM-DD strings between from_dt and to_dt."""
    days = []
    current = from_dt.replace(hour=0, minute=0, second=0, microsecond=0)
    end = to_dt.replace(hour=0, minute=0, second=0, microsecond=0)
    while current <= end:
        days.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)
    return days or [from_dt.strftime("%Y-%m-%d")]


class DemoDataService:
    """Generates realistic demo data for all API endpoints."""

    # ------------------------------------------------------------------
    # Overview
    # ------------------------------------------------------------------
    def get_overview(self, from_dt: datetime, to_dt: datetime) -> Dict:
        days = _date_range_days(from_dt, to_dt)
        rng = random.Random(_hash_seed(f"overview-{days[0]}"))

        total_events = rng.randint(8000, 25000)
        total_mentions = total_events * rng.randint(3, 8)
        total_articles = int(total_mentions * 0.6)

        timeline = []
        for d in days:
            day_rng = random.Random(_hash_seed(f"ts-{d}"))
            count = int(total_events / max(len(days), 1) * day_rng.uniform(0.5, 1.8))
            timeline.append({
                "date": d,
                "count": count,
                "avg_tone": round(day_rng.uniform(-3.0, 2.0), 2),
                "avg_goldstein": round(day_rng.uniform(-4.0, 6.0), 2),
            })

        top_topics = []
        for t in TOPICS[:8]:
            t_rng = random.Random(_hash_seed(f"tt-{t}-{days[0]}"))
            top_topics.append({
                "topic": t,
                "count": t_rng.randint(200, 3000),
                "avg_tone": round(t_rng.uniform(-2.5, 1.5), 2),
            })
        top_topics.sort(key=lambda x: x["count"], reverse=True)

        # Category distribution
        cats = []
        total_cat = 0
        for code, (name, is_conflict) in CAMEO_CATEGORIES.items():
            c_rng = random.Random(_hash_seed(f"cat-{code}-{days[0]}"))
            cnt = c_rng.randint(50, 2000) if not is_conflict else c_rng.randint(30, 800)
            total_cat += cnt
            cats.append({"category": code, "category_name": name,
                         "count": cnt, "is_conflict": is_conflict})
        for c in cats:
            c["pct"] = round(c["count"] / max(total_cat, 1) * 100, 1)

        return {
            "total_events": total_events,
            "total_mentions": total_mentions,
            "total_articles": total_articles,
            "active_countries": len(COUNTRIES),
            "avg_tone": round(rng.uniform(-1.5, 0.5), 2),
            "avg_goldstein": round(rng.uniform(-1.0, 4.0), 2),
            "event_timeline": timeline,
            "top_topics": top_topics,
            "category_distribution": cats,
            "quad_class_summary": {
                "Verbal Cooperation": rng.randint(3000, 8000),
                "Material Cooperation": rng.randint(1000, 3000),
                "Verbal Conflict": rng.randint(800, 2500),
                "Material Conflict": rng.randint(400, 1500),
            },
        }

    # ------------------------------------------------------------------
    # Events
    # ------------------------------------------------------------------
    def get_events(self, from_dt: datetime, to_dt: datetime,
                   country: Optional[str] = None, category: Optional[str] = None,
                   page: int = 1, limit: int = 50) -> Dict:
        rng = random.Random(_hash_seed(f"events-{page}-{country}"))
        total = rng.randint(500, 5000)
        items = []
        for i in range((page - 1) * limit, min(page * limit, total)):
            c = rng.choice(COUNTRIES)
            if country and c[0] != country.upper():
                c = next((x for x in COUNTRIES if x[0] == country.upper()), c)
            cat_code = rng.choice(list(CAMEO_CATEGORIES.keys()))
            cat_name, is_conflict = CAMEO_CATEGORIES[cat_code]
            event_date = from_dt + timedelta(seconds=rng.randint(0, max(1, int((to_dt - from_dt).total_seconds()))))
            items.append({
                "GlobalEventID": 1000000000 + i,
                "SQLDATE": int(event_date.strftime("%Y%m%d")),
                "EventDate": event_date.strftime("%Y-%m-%d"),
                "Actor1Name": rng.choice(["GOVERNMENT", "MILITARY", "CITIZEN", "POLICE", "MEDIA", "OPPOSITION", "BUSINESS"]),
                "Actor1CountryCode": c[0],
                "Actor2Name": rng.choice(["GOVERNMENT", "UNITED NATIONS", "CITIZEN", "REBEL", "NGO", "BUSINESS"]),
                "Actor2CountryCode": rng.choice(COUNTRIES)[0],
                "EventCode": f"{cat_code}{rng.randint(1,9)}",
                "EventRootCode": cat_code,
                "EventCategoryName": cat_name,
                "QuadClass": rng.choice([1, 2, 3, 4]),
                "GoldsteinScale": round(rng.uniform(-10, 10), 1),
                "NumMentions": rng.randint(1, 200),
                "NumSources": rng.randint(1, 50),
                "NumArticles": rng.randint(1, 80),
                "AvgTone": round(rng.uniform(-8, 6), 2),
                "ActionGeo_CountryCode": c[0],
                "ActionGeo_FullName": c[1],
                "ActionGeo_Lat": round(c[2] + rng.uniform(-2, 2), 4),
                "ActionGeo_Long": round(c[3] + rng.uniform(-2, 2), 4),
                "SOURCEURL": f"https://news.example.com/article-{1000000000 + i}",
            })
        return {
            "items": items,
            "total": total,
            "page": page,
            "page_size": limit,
            "total_pages": math.ceil(total / limit),
        }

    def get_event_by_id(self, event_id: int) -> Dict:
        rng = random.Random(event_id)
        c = rng.choice(COUNTRIES)
        cat_code = rng.choice(list(CAMEO_CATEGORIES.keys()))
        return {
            "GlobalEventID": event_id,
            "SQLDATE": 20261123,
            "EventDate": "2026-11-23",
            "Actor1Name": "GOVERNMENT",
            "Actor1CountryCode": c[0],
            "Actor2Name": "CITIZEN",
            "Actor2CountryCode": c[0],
            "EventCode": f"{cat_code}1",
            "EventRootCode": cat_code,
            "EventCategoryName": CAMEO_CATEGORIES[cat_code][0],
            "QuadClass": rng.choice([1, 2, 3, 4]),
            "GoldsteinScale": round(rng.uniform(-10, 10), 1),
            "NumMentions": rng.randint(1, 200),
            "NumSources": rng.randint(1, 50),
            "NumArticles": rng.randint(1, 80),
            "AvgTone": round(rng.uniform(-8, 6), 2),
            "ActionGeo_CountryCode": c[0],
            "ActionGeo_FullName": c[1],
            "ActionGeo_Lat": c[2],
            "ActionGeo_Long": c[3],
            "SOURCEURL": f"https://news.example.com/article-{event_id}",
        }

    def get_event_mentions(self, event_id: int, limit: int = 50) -> Dict:
        rng = random.Random(event_id + 999)
        mentions = []
        for i in range(min(rng.randint(3, 20), limit)):
            mentions.append({
                "MentionID": f"{event_id}-{i}",
                "EventID": event_id,
                "MentionTimeDate": "20261123120000",
                "MentionType": rng.choice([1, 2, 3]),
                "MentionSourceName": rng.choice(["reuters.com", "bbc.co.uk", "cnn.com", "aljazeera.com", "nytimes.com"]),
                "MentionDocTone": round(rng.uniform(-5, 5), 2),
            })
        return {"mentions": mentions, "total": len(mentions)}

    # ------------------------------------------------------------------
    # Topics
    # ------------------------------------------------------------------
    def get_topics(self, from_dt: datetime, to_dt: datetime,
                   country: Optional[str] = None) -> Dict:
        rng = random.Random(_hash_seed(f"topics-{country}"))
        topics = []
        for t in TOPICS:
            topics.append({
                "topic": t,
                "count": rng.randint(100, 5000),
                "avg_tone": round(rng.uniform(-3, 2), 2),
            })
        topics.sort(key=lambda x: x["count"], reverse=True)
        return {"data": topics, "total": len(topics)}

    def get_topic_trend(self, topic: str, from_dt: datetime, to_dt: datetime) -> Dict:
        days = _date_range_days(from_dt, to_dt)
        rng = random.Random(_hash_seed(f"trend-{topic}"))
        base = rng.randint(50, 400)
        trend = []
        for d in days:
            day_rng = random.Random(_hash_seed(f"tt-{topic}-{d}"))
            trend.append({
                "date": d,
                "count": int(base * day_rng.uniform(0.4, 2.2)),
                "avg_tone": round(day_rng.uniform(-3, 2), 2),
            })
        return {"topic": topic, "trend": trend}

    def get_topic_countries(self, topic: str, from_dt: datetime, to_dt: datetime) -> Dict:
        rng = random.Random(_hash_seed(f"tc-{topic}"))
        countries = []
        for c in COUNTRIES[:10]:
            countries.append({
                "country_code": c[0],
                "country_name": c[1],
                "count": rng.randint(10, 800),
                "avg_tone": round(rng.uniform(-3, 2), 2),
            })
        countries.sort(key=lambda x: x["count"], reverse=True)
        return {"topic": topic, "countries": countries}

    # ------------------------------------------------------------------
    # Countries
    # ------------------------------------------------------------------
    def get_country_overview(self, country_code: str, from_dt: datetime, to_dt: datetime) -> Dict:
        c = next((x for x in COUNTRIES if x[0] == country_code), COUNTRIES[0])
        days = _date_range_days(from_dt, to_dt)
        rng = random.Random(_hash_seed(f"co-{country_code}"))

        timeline = []
        for d in days:
            day_rng = random.Random(_hash_seed(f"ctl-{country_code}-{d}"))
            timeline.append({
                "date": d,
                "count": day_rng.randint(50, 500),
                "avg_tone": round(day_rng.uniform(-3, 2), 2),
                "avg_goldstein": round(day_rng.uniform(-5, 7), 2),
            })

        top_topics = []
        for t in TOPICS[:5]:
            t_rng = random.Random(_hash_seed(f"ctt-{country_code}-{t}"))
            top_topics.append({"topic": t, "count": t_rng.randint(20, 500), "avg_tone": round(t_rng.uniform(-2, 2), 2)})

        cats = []
        for code, (name, is_conf) in list(CAMEO_CATEGORIES.items())[:10]:
            cats.append({"category": code, "category_name": name, "count": rng.randint(10, 300),
                         "is_conflict": is_conf, "pct": 0})
        total_cat = sum(x["count"] for x in cats)
        for x in cats:
            x["pct"] = round(x["count"] / max(total_cat, 1) * 100, 1)

        conflict_tl = []
        for d in days:
            d_rng = random.Random(_hash_seed(f"cfl-{country_code}-{d}"))
            conflict_tl.append({"date": d, "cooperation": d_rng.randint(30, 200), "conflict": d_rng.randint(10, 80)})

        return {
            "country_code": c[0],
            "country_name": c[1],
            "total_events": rng.randint(500, 5000),
            "total_mentions": rng.randint(2000, 20000),
            "avg_tone": round(rng.uniform(-2, 1.5), 2),
            "avg_goldstein": round(rng.uniform(-2, 5), 2),
            "event_timeline": timeline,
            "top_topics": top_topics,
            "category_distribution": cats,
            "conflict_timeline": conflict_tl,
            "recent_events": self.get_events(from_dt, to_dt, country=country_code, page=1, limit=10)["items"],
        }

    def get_country_regions(self, country_code: str, from_dt: datetime, to_dt: datetime) -> Dict:
        rng = random.Random(_hash_seed(f"reg-{country_code}"))
        regions = []
        region_names = {
            "US": ["California", "Texas", "New York", "Florida", "Illinois"],
            "UK": ["England", "Scotland", "Wales", "Northern Ireland"],
            "IN": ["Delhi", "Maharashtra", "Karnataka", "Tamil Nadu", "Gujarat"],
        }
        names = region_names.get(country_code, [f"Region {i}" for i in range(1, 6)])
        for name in names:
            regions.append({
                "region_name": name,
                "event_count": rng.randint(50, 500),
                "avg_tone": round(rng.uniform(-2, 2), 2),
            })
        return {"country_code": country_code, "regions": regions}

    # ------------------------------------------------------------------
    # Comparison
    # ------------------------------------------------------------------
    def compare_countries(self, codes: List[str], from_dt: datetime, to_dt: datetime) -> Dict:
        countries = []
        for code in codes:
            c = next((x for x in COUNTRIES if x[0] == code), None)
            if not c:
                c = (code, code, 0, 0)
            rng = random.Random(_hash_seed(f"cmp-{code}"))
            countries.append({
                "country_code": c[0],
                "country_name": c[1],
                "total_events": rng.randint(500, 8000),
                "total_mentions": rng.randint(2000, 30000),
                "avg_tone": round(rng.uniform(-3, 2), 2),
                "avg_goldstein": round(rng.uniform(-3, 6), 2),
            })
        return {"countries": countries}

    # ------------------------------------------------------------------
    # Analytics
    # ------------------------------------------------------------------
    def get_category_metrics(self, from_dt: datetime, to_dt: datetime,
                             country: Optional[str] = None) -> Dict:
        rng = random.Random(_hash_seed(f"catm-{country}"))
        cats = []
        total = 0
        for code, (name, is_conflict) in CAMEO_CATEGORIES.items():
            cnt = rng.randint(50, 2000) if not is_conflict else rng.randint(20, 800)
            total += cnt
            cats.append({"category": code, "category_name": name, "count": cnt, "is_conflict": is_conflict})
        for c in cats:
            c["pct"] = round(c["count"] / max(total, 1) * 100, 1)
        return {"data": cats, "total": total}

    def get_tone_metrics(self, from_dt: datetime, to_dt: datetime,
                         country: Optional[str] = None, topic: Optional[str] = None) -> Dict:
        days = _date_range_days(from_dt, to_dt)
        trend = []
        for d in days:
            d_rng = random.Random(_hash_seed(f"tone-{d}-{country}"))
            avg = d_rng.uniform(-3, 2)
            trend.append({
                "date": d,
                "avg_tone": round(avg, 2),
                "min_tone": round(avg - d_rng.uniform(2, 5), 2),
                "max_tone": round(avg + d_rng.uniform(2, 5), 2),
                "count": d_rng.randint(100, 2000),
            })
        return {"data": trend, "total": len(trend)}

    def get_cooperation_conflict(self, from_dt: datetime, to_dt: datetime,
                                  country: Optional[str] = None) -> Dict:
        rng = random.Random(_hash_seed(f"coopconf-{country}"))
        coop = rng.randint(3000, 10000)
        conf = rng.randint(1000, 5000)
        neut = rng.randint(500, 2000)
        total = coop + conf + neut
        return {
            "cooperation_count": coop,
            "conflict_count": conf,
            "neutral_count": neut,
            "cooperation_pct": round(coop / total * 100, 1),
            "conflict_pct": round(conf / total * 100, 1),
            "avg_goldstein": round(rng.uniform(-1, 4), 2),
        }

    def get_anomalies(self, from_dt: datetime, to_dt: datetime,
                      country: Optional[str] = None, min_z_score: float = 2.5) -> Dict:
        rng = random.Random(_hash_seed(f"anom-{country}"))
        anomalies = []
        for i in range(rng.randint(3, 8)):
            z = round(rng.uniform(2.5, 6.0), 2)
            baseline = rng.randint(50, 300)
            current = int(baseline * rng.uniform(2.5, 8.0))
            severity = "critical" if z > 4.5 else "high" if z > 3.5 else "medium"
            anomalies.append({
                "id": f"anomaly-{i}",
                "type": rng.choice(["topic", "country", "category"]),
                "label": rng.choice(TOPICS + [c[0] for c in COUNTRIES[:6]]),
                "country_code": rng.choice(COUNTRIES[:6])[0],
                "baseline": baseline,
                "current": current,
                "pct_change": round((current - baseline) / max(baseline, 1) * 100, 0),
                "z_score": z,
                "severity": severity,
                "spike_started": (from_dt + timedelta(hours=rng.randint(0, 72))).isoformat(),
                "event_count": current,
            })
        anomalies.sort(key=lambda x: x["z_score"], reverse=True)
        return {"data": anomalies, "total": len(anomalies)}

    # ------------------------------------------------------------------
    # Map
    # ------------------------------------------------------------------
    def get_map_events(self, from_dt: datetime, to_dt: datetime,
                       category: Optional[str] = None, limit: int = 200) -> Dict:
        rng = random.Random(_hash_seed(f"map-{from_dt.date()}"))
        markers = []
        for i in range(min(limit, 150)):
            c = rng.choice(COUNTRIES)
            cat_code = rng.choice(list(CAMEO_CATEGORIES.keys()))
            markers.append({
                "lat": round(c[2] + rng.uniform(-5, 5), 4),
                "lng": round(c[3] + rng.uniform(-5, 5), 4),
                "event_id": 1000000000 + i,
                "event_code": f"{cat_code}{rng.randint(1,9)}",
                "event_name": CAMEO_CATEGORIES[cat_code][0],
                "country_code": c[0],
                "avg_tone": round(rng.uniform(-5, 5), 2),
                "goldstein": round(rng.uniform(-8, 8), 1),
                "date": (from_dt + timedelta(hours=rng.randint(0, 168))).strftime("%Y-%m-%d"),
                "source_url": f"https://news.example.com/article-{i}",
            })
        return {"data": markers, "total": len(markers)}

    def get_map_countries(self, from_dt: datetime, to_dt: datetime) -> Dict:
        rng = random.Random(_hash_seed(f"mapc-{from_dt.date()}"))
        countries = []
        for c in COUNTRIES:
            val = rng.randint(50, 3000)
            countries.append({
                "country_code": c[0],
                "country_name": c[1],
                "value": val,
                "normalized": round(val / 3000, 3),
            })
        return {"data": countries, "total": len(countries)}

    # ------------------------------------------------------------------
    # Search
    # ------------------------------------------------------------------
    def search_events(self, q: str, from_dt: datetime, to_dt: datetime,
                      page: int = 1, limit: int = 50) -> Dict:
        # Reuse events but filter by query
        events = self.get_events(from_dt, to_dt, page=page, limit=limit)
        events["query"] = q
        return events

    # ------------------------------------------------------------------
    # Regions
    # ------------------------------------------------------------------
    def get_region_overview(self, region_id: str, from_dt: datetime, to_dt: datetime) -> Dict:
        rng = random.Random(_hash_seed(f"reg-{region_id}"))
        return {
            "region_id": region_id,
            "region_name": region_id.replace("-", " ").title(),
            "total_events": rng.randint(100, 2000),
            "avg_tone": round(rng.uniform(-2, 2), 2),
            "avg_goldstein": round(rng.uniform(-3, 5), 2),
        }
