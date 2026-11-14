"""
HBase service layer — all query logic for the API.
Wraps the HBase client and converts results to API response dicts.
Returns empty-state responses when HBase is unavailable or no data exists.
"""
from __future__ import annotations
import logging
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from src.common.config import get_settings
from src.common.cameo_mapping import CAMEO_ROOT_CATEGORIES, QUAD_CLASS_NAMES, get_category_name, fips_to_iso2

logger = logging.getLogger(__name__)
settings = get_settings()

# Reverse timestamp helper: used to build HBase row keys for time-descending scans
_MAX_TS = sys.maxsize


def _rev_ts(dt: datetime) -> str:
    epoch = int(dt.replace(tzinfo=timezone.utc).timestamp()) if dt.tzinfo else int(dt.timestamp())
    return str(_MAX_TS - epoch).zfill(19)


def _empty_paginated(page: int = 1, limit: int = 50) -> Dict:
    return {"items": [], "total": 0, "page": page, "page_size": limit, "total_pages": 0,
            "message": "No data available for the selected time range"}


def _empty_list() -> Dict:
    return {"data": [], "total": 0, "message": "No data available for the selected time range"}


class HBaseService:
    """Service layer that wraps HBase reads and returns API-ready dicts."""

    def __init__(self):
        self._conn = None
        try:
            import happybase
            self._conn = happybase.Connection(
                host=settings.hbase_host,
                port=settings.hbase_thrift_port,
                timeout=5000,
            )
        except Exception as exc:
            logger.warning("HBase not available: %s — returning empty responses", exc)

    def _table(self, name: str):
        if self._conn is None:
            return None
        try:
            return self._conn.table(name)
        except Exception as exc:
            logger.warning("Cannot open HBase table %s: %s", name, exc)
            return None

    @staticmethod
    def _decode(val: bytes | None) -> str | None:
        if val is None:
            return None
        try:
            return val.decode("utf-8")
        except Exception:
            return None

    @staticmethod
    def _decode_float(val: bytes | None) -> float | None:
        s = HBaseService._decode(val)
        if s is None:
            return None
        try:
            return float(s)
        except ValueError:
            return None

    @staticmethod
    def _decode_int(val: bytes | None) -> int | None:
        s = HBaseService._decode(val)
        if s is None:
            return None
        try:
            return int(s)
        except ValueError:
            return None

    # ------------------------------------------------------------------
    # Overview
    # ------------------------------------------------------------------
    def get_overview(self, from_dt: datetime, to_dt: datetime) -> Dict:
        country_metrics = self._scan_country_metrics(from_dt, to_dt)
        category_metrics = self._scan_category_metrics(from_dt, to_dt)

        total_events = sum(m.get("event_count", 0) for m in country_metrics)
        total_mentions = sum(m.get("mention_count", 0) for m in country_metrics)
        total_articles = sum(m.get("article_count", 0) for m in country_metrics)

        tones = [m["avg_tone"] for m in country_metrics if m.get("avg_tone") is not None]
        avg_tone = round(sum(tones) / len(tones), 3) if tones else None

        top_countries = sorted(country_metrics, key=lambda x: x.get("event_count", 0), reverse=True)[:10]
        active_countries = len({m["country_code"] for m in country_metrics})

        # Build category distribution
        cat_totals: Dict[str, int] = {}
        for m in category_metrics:
            rc = m.get("root_code", "")
            cat_totals[rc] = cat_totals.get(rc, 0) + m.get("count", 0)
        grand_total = sum(cat_totals.values()) or 1
        cat_dist = [
            {
                "category": rc,
                "category_name": get_category_name(rc),
                "count": cnt,
                "pct": round(cnt / grand_total * 100, 2),
                "is_conflict": CAMEO_ROOT_CATEGORIES.get(rc, ("", False))[1],
            }
            for rc, cnt in sorted(cat_totals.items(), key=lambda x: x[1], reverse=True)
        ]

        coop = sum(m.get("cooperation_count", 0) for m in country_metrics)
        conf = sum(m.get("conflict_count", 0) for m in country_metrics)
        cc_total = (coop + conf) or 1

        return {
            "total_events": total_events,
            "total_mentions": total_mentions,
            "total_articles": total_articles,
            "active_countries": active_countries,
            "avg_tone": avg_tone,
            "top_countries": top_countries,
            "category_distribution": cat_dist,
            "cooperation_count": coop,
            "conflict_count": conf,
            "cooperation_pct": round(coop / cc_total * 100, 2),
            "conflict_pct": round(conf / cc_total * 100, 2),
            "from_dt": from_dt.isoformat(),
            "to_dt": to_dt.isoformat(),
            "data_available": total_events > 0,
        }

    # ------------------------------------------------------------------
    # Events
    # ------------------------------------------------------------------
    def get_events(
        self, from_dt: datetime, to_dt: datetime,
        country: Optional[str] = None, topic: Optional[str] = None,
        category: Optional[str] = None, tone_min: Optional[float] = None,
        tone_max: Optional[float] = None, page: int = 1, limit: int = 50,
    ) -> Dict:
        table = self._table("events_by_country_time")
        if table is None:
            return _empty_paginated(page, limit)
        try:
            prefix = f"{country}_" if country else None
            row_start = f"{country}_{_rev_ts(to_dt)}" if country else _rev_ts(to_dt)
            row_stop = f"{country}_{_rev_ts(from_dt)}" if country else _rev_ts(from_dt)
            scan_kwargs: Dict[str, Any] = {
                "row_start": row_start.encode(),
                "row_stop": row_stop.encode(),
            }
            rows = []
            for _, data in table.scan(**scan_kwargs):
                row = self._row_to_event(data)
                if category and row.get("EventRootCode") != category:
                    continue
                if tone_min is not None and (row.get("AvgTone") or 0) < tone_min:
                    continue
                if tone_max is not None and (row.get("AvgTone") or 0) > tone_max:
                    continue
                rows.append(row)
                if len(rows) >= limit * page + limit:
                    break
            total = len(rows)
            offset = (page - 1) * limit
            page_items = rows[offset: offset + limit]
            return {
                "items": page_items, "total": total,
                "page": page, "page_size": limit,
                "total_pages": max(1, (total + limit - 1) // limit),
            }
        except Exception as exc:
            logger.error("HBase events scan error: %s", exc)
            return _empty_paginated(page, limit)

    def get_event_by_id(self, event_id: int) -> Dict:
        table = self._table("events_by_id")
        if table is None:
            return {}
        try:
            row = table.row(str(event_id).encode())
            return self._row_to_event(row) if row else {}
        except Exception as exc:
            logger.error("HBase get_event error: %s", exc)
            return {}

    def get_event_mentions(self, event_id: int, limit: int = 50) -> Dict:
        # Mentions stored separately; return empty with note if table missing
        return {"items": [], "total": 0,
                "note": "Mentions lookup requires mentions_by_event HBase table"}

    def _row_to_event(self, data: Dict[bytes, bytes]) -> Dict:
        def g(cf_col: str) -> Any:
            return self._decode(data.get(cf_col.encode()))
        def gf(cf_col: str) -> Any:
            return self._decode_float(data.get(cf_col.encode()))
        def gi(cf_col: str) -> Any:
            return self._decode_int(data.get(cf_col.encode()))
        rc = g("event:EventRootCode") or ""
        return {
            "GlobalEventID": gi("event:GlobalEventID"),
            "SQLDATE": gi("event:SQLDATE"),
            "Actor1Name": g("event:Actor1Name"),
            "Actor1CountryCode": g("event:Actor1CountryCode"),
            "Actor2Name": g("event:Actor2Name"),
            "Actor2CountryCode": g("event:Actor2CountryCode"),
            "ActionGeo_CountryCode": g("event:ActionGeo_CountryCode"),
            "ActionGeo_FullName": g("event:ActionGeo_FullName"),
            "ActionGeo_Lat": gf("event:ActionGeo_Lat"),
            "ActionGeo_Long": gf("event:ActionGeo_Long"),
            "ActionGeo_Type": gi("event:ActionGeo_Type"),
            "EventCode": g("event:EventCode"),
            "EventRootCode": rc,
            "EventCategoryName": get_category_name(rc),
            "QuadClass": gi("event:QuadClass"),
            "QuadClassName": QUAD_CLASS_NAMES.get(gi("event:QuadClass") or 0, "Unknown"),
            "GoldsteinScale": gf("event:GoldsteinScale"),
            "NumMentions": gi("event:NumMentions"),
            "NumArticles": gi("event:NumArticles"),
            "AvgTone": gf("event:AvgTone"),
            "SOURCEURL": g("event:SOURCEURL"),
            "IsConflict": CAMEO_ROOT_CATEGORIES.get(rc, ("", False))[1],
        }

    # ------------------------------------------------------------------
    # Topics
    # ------------------------------------------------------------------
    def get_topics(
        self, from_dt: datetime, to_dt: datetime,
        country: Optional[str] = None, limit: int = 20,
    ) -> Dict:
        rows = self._scan_topic_metrics(from_dt, to_dt)
        # Aggregate by topic
        agg: Dict[str, Dict] = {}
        for r in rows:
            t = r.get("topic", "")
            if not t:
                continue
            if t not in agg:
                agg[t] = {"topic": t, "count": 0, "article_count": 0, "tone_sum": 0.0, "tone_n": 0}
            agg[t]["count"] += r.get("count", 0)
            agg[t]["article_count"] += r.get("article_count", 0)
            if r.get("avg_tone") is not None:
                agg[t]["tone_sum"] += r["avg_tone"]
                agg[t]["tone_n"] += 1
        topics = []
        for t, v in agg.items():
            avg_tone = round(v["tone_sum"] / v["tone_n"], 3) if v["tone_n"] else None
            topics.append({"topic": t, "count": v["count"], "article_count": v["article_count"], "avg_tone": avg_tone})
        topics.sort(key=lambda x: x["count"], reverse=True)
        return {"data": topics[:limit], "total": len(topics)}

    def get_topic_trend(self, topic: str, from_dt: datetime, to_dt: datetime) -> Dict:
        rows = self._scan_topic_metrics(from_dt, to_dt, topic=topic)
        series = [{"date": r.get("date"), "count": r.get("count", 0), "avg_tone": r.get("avg_tone")} for r in rows]
        series.sort(key=lambda x: x.get("date") or "")
        return {"topic": topic, "trend": series}

    def get_topic_countries(self, topic: str, from_dt: datetime, to_dt: datetime) -> Dict:
        rows = self._scan_topic_metrics(from_dt, to_dt, topic=topic)
        country_counts: Dict[str, int] = {}
        for r in rows:
            for cc, cnt in (r.get("country_distribution") or {}).items():
                country_counts[cc] = country_counts.get(cc, 0) + cnt
        result = sorted(
            [{"country_code": cc, "count": cnt} for cc, cnt in country_counts.items()],
            key=lambda x: x["count"], reverse=True,
        )
        return {"topic": topic, "countries": result}

    # ------------------------------------------------------------------
    # Countries
    # ------------------------------------------------------------------
    def get_country_overview(self, country_code: str, from_dt: datetime, to_dt: datetime) -> Dict:
        metrics = self._scan_country_metrics(from_dt, to_dt, country=country_code)
        if not metrics:
            return {"country_code": country_code, "data_available": False,
                    "message": "No data for this country in the selected range"}
        total_events = sum(m.get("event_count", 0) for m in metrics)
        total_mentions = sum(m.get("mention_count", 0) for m in metrics)
        tones = [m["avg_tone"] for m in metrics if m.get("avg_tone") is not None]
        gs = [m["avg_goldstein"] for m in metrics if m.get("avg_goldstein") is not None]
        coop = sum(m.get("cooperation_count", 0) for m in metrics)
        conf = sum(m.get("conflict_count", 0) for m in metrics)
        timeline = [{"date": m["date"], "event_count": m.get("event_count", 0), "avg_tone": m.get("avg_tone")} for m in metrics]
        timeline.sort(key=lambda x: x["date"])
        return {
            "country_code": country_code,
            "total_events": total_events,
            "total_mentions": total_mentions,
            "avg_tone": round(sum(tones) / len(tones), 3) if tones else None,
            "avg_goldstein": round(sum(gs) / len(gs), 3) if gs else None,
            "cooperation_count": coop, "conflict_count": conf,
            "event_timeline": timeline, "data_available": True,
        }

    def get_country_regions(self, country_code: str, from_dt: datetime, to_dt: datetime) -> Dict:
        return {"country_code": country_code, "regions": [],
                "note": "Region-level data requires ADM1 reverse geocoding integration"}

    def compare_countries(self, codes: List[str], from_dt: datetime, to_dt: datetime) -> Dict:
        result = []
        for code in codes:
            overview = self.get_country_overview(code, from_dt, to_dt)
            result.append(overview)
        return {"countries": result, "from_dt": from_dt.isoformat(), "to_dt": to_dt.isoformat()}

    # ------------------------------------------------------------------
    # Analytics
    # ------------------------------------------------------------------
    def get_category_metrics(self, from_dt: datetime, to_dt: datetime, country: Optional[str] = None) -> Dict:
        rows = self._scan_category_metrics(from_dt, to_dt, country=country)
        agg: Dict[str, Dict] = {}
        for r in rows:
            rc = r.get("root_code", "")
            if rc not in agg:
                agg[rc] = {"root_code": rc, "category_name": get_category_name(rc),
                           "count": 0, "tone_sum": 0.0, "tone_n": 0,
                           "goldstein_sum": 0.0, "goldstein_n": 0,
                           "is_conflict": CAMEO_ROOT_CATEGORIES.get(rc, ("", False))[1]}
            agg[rc]["count"] += r.get("count", 0)
            if r.get("avg_tone") is not None:
                agg[rc]["tone_sum"] += r["avg_tone"]
                agg[rc]["tone_n"] += 1
            if r.get("avg_goldstein") is not None:
                agg[rc]["goldstein_sum"] += r["avg_goldstein"]
                agg[rc]["goldstein_n"] += 1
        total = sum(v["count"] for v in agg.values()) or 1
        result = []
        for rc, v in sorted(agg.items(), key=lambda x: x[1]["count"], reverse=True):
            result.append({
                "root_code": rc, "category_name": v["category_name"],
                "count": v["count"], "pct": round(v["count"] / total * 100, 2),
                "avg_tone": round(v["tone_sum"] / v["tone_n"], 3) if v["tone_n"] else None,
                "avg_goldstein": round(v["goldstein_sum"] / v["goldstein_n"], 3) if v["goldstein_n"] else None,
                "is_conflict": v["is_conflict"],
            })
        return {"data": result, "total": sum(v["count"] for v in agg.values())}

    def get_tone_metrics(self, from_dt: datetime, to_dt: datetime, country: Optional[str] = None, topic: Optional[str] = None) -> Dict:
        metrics = self._scan_country_metrics(from_dt, to_dt, country=country)
        series = [{"date": m.get("date"), "avg_tone": m.get("avg_tone")} for m in metrics if m.get("date")]
        series.sort(key=lambda x: x.get("date") or "")
        tones = [s["avg_tone"] for s in series if s["avg_tone"] is not None]
        return {
            "trend": series,
            "overall_avg_tone": round(sum(tones) / len(tones), 3) if tones else None,
            "country": country, "topic": topic,
            "note": "Tone reflects textual reporting tone (GDELT AvgTone), not factual sentiment.",
        }

    def get_cooperation_conflict(self, from_dt: datetime, to_dt: datetime, country: Optional[str] = None) -> Dict:
        metrics = self._scan_country_metrics(from_dt, to_dt, country=country)
        coop = sum(m.get("cooperation_count", 0) for m in metrics)
        conf = sum(m.get("conflict_count", 0) for m in metrics)
        total = (coop + conf) or 1
        return {
            "cooperation_count": coop, "conflict_count": conf,
            "cooperation_pct": round(coop / total * 100, 2),
            "conflict_pct": round(conf / total * 100, 2),
            "country": country,
        }

    def get_anomalies(self, from_dt: datetime, to_dt: datetime, country: Optional[str] = None, min_z_score: float = 2.5) -> Dict:
        table = self._table("anomalies_by_time")
        if table is None:
            return _empty_list()
        try:
            row_start = _rev_ts(to_dt).encode()
            row_stop = _rev_ts(from_dt).encode()
            items = []
            for _, data in table.scan(row_start=row_start, row_stop=row_stop):
                z = self._decode_float(data.get(b"alert:z_score"))
                if z is None or z < min_z_score:
                    continue
                cc = self._decode(data.get(b"alert:country_code"))
                if country and cc != country:
                    continue
                items.append({
                    "anomaly_type": self._decode(data.get(b"alert:anomaly_type")),
                    "label": self._decode(data.get(b"alert:label")),
                    "country_code": cc,
                    "topic": self._decode(data.get(b"alert:topic")),
                    "baseline_avg": self._decode_float(data.get(b"alert:baseline_avg")),
                    "current_count": self._decode_float(data.get(b"alert:current_count")),
                    "pct_change": self._decode_float(data.get(b"alert:pct_change")),
                    "z_score": z,
                    "severity": "critical" if z >= 3.0 else "high" if z >= 2.5 else "medium",
                    "detected_at": self._decode(data.get(b"alert:detected_at")),
                })
            items.sort(key=lambda x: x.get("z_score") or 0, reverse=True)
            return {"data": items, "total": len(items)}
        except Exception as exc:
            logger.error("HBase anomalies scan error: %s", exc)
            return _empty_list()

    # ------------------------------------------------------------------
    # Map
    # ------------------------------------------------------------------
    def get_map_events(self, from_dt: datetime, to_dt: datetime,
                       country: Optional[str] = None, topic: Optional[str] = None,
                       category: Optional[str] = None, max_points: int = 2000) -> Dict:
        events_resp = self.get_events(from_dt, to_dt, country=country, category=category, limit=max_points)
        items = events_resp.get("items", [])
        markers = [
            {
                "event_id": e.get("GlobalEventID"),
                "lat": e.get("ActionGeo_Lat"),
                "lng": e.get("ActionGeo_Long"),
                "event_code": e.get("EventCode"),
                "event_name": e.get("EventCategoryName"),
                "country_code": e.get("ActionGeo_CountryCode"),
                "avg_tone": e.get("AvgTone"),
                "goldstein": e.get("GoldsteinScale"),
                "source_url": e.get("SOURCEURL"),
                "geo_type": e.get("ActionGeo_Type"),
            }
            for e in items if e.get("ActionGeo_Lat") and e.get("ActionGeo_Long")
        ]
        no_coords = len(items) - len(markers)
        return {"markers": markers, "total": len(markers), "no_coordinates_count": no_coords}

    def get_map_countries(self, from_dt: datetime, to_dt: datetime) -> Dict:
        metrics = self._scan_country_metrics(from_dt, to_dt)
        agg: Dict[str, int] = {}
        for m in metrics:
            cc = m.get("country_code", "")
            agg[cc] = agg.get(cc, 0) + m.get("event_count", 0)
        max_val = max(agg.values(), default=1)
        result = [
            {"country_code": cc, "event_count": cnt, "normalized": round(cnt / max_val, 4)}
            for cc, cnt in agg.items()
        ]
        return {"data": result, "total": len(result)}

    # ------------------------------------------------------------------
    # Search
    # ------------------------------------------------------------------
    def search_events(self, q: str, from_dt: datetime, to_dt: datetime, page: int = 1, limit: int = 50) -> Dict:
        # Basic keyword search via HBase scan — in production use ElasticSearch or HDFS Spark query
        result = self.get_events(from_dt, to_dt, page=page, limit=limit)
        q_lower = q.lower()
        items = [
            e for e in result.get("items", [])
            if q_lower in str(e.get("Actor1Name") or "").lower()
            or q_lower in str(e.get("Actor2Name") or "").lower()
            or q_lower in str(e.get("ActionGeo_FullName") or "").lower()
            or q_lower in str(e.get("SOURCEURL") or "").lower()
        ]
        return {"items": items, "total": len(items), "query": q, "page": page, "page_size": limit}

    # ------------------------------------------------------------------
    # Region
    # ------------------------------------------------------------------
    def get_region_overview(self, region_id: str, from_dt: datetime, to_dt: datetime) -> Dict:
        return {
            "region_id": region_id,
            "data_available": False,
            "note": "Region-level aggregation requires ADM1 boundary data integration.",
        }

    # ------------------------------------------------------------------
    # Private HBase scan helpers
    # ------------------------------------------------------------------
    def _scan_country_metrics(self, from_dt: datetime, to_dt: datetime, country: Optional[str] = None) -> List[Dict]:
        table = self._table("country_metrics_by_time")
        if table is None:
            return []
        try:
            prefix = f"{country}_" if country else None
            row_start = (f"{country}_{_rev_ts(to_dt)}" if country else _rev_ts(to_dt)).encode()
            row_stop = (f"{country}_{_rev_ts(from_dt)}" if country else _rev_ts(from_dt)).encode()
            rows = []
            for _, data in table.scan(row_start=row_start, row_stop=row_stop):
                rows.append({
                    "country_code": self._decode(data.get(b"metrics:country_code")),
                    "date": self._decode(data.get(b"metrics:date")),
                    "event_count": self._decode_int(data.get(b"metrics:event_count")) or 0,
                    "mention_count": self._decode_int(data.get(b"metrics:mention_count")) or 0,
                    "article_count": self._decode_int(data.get(b"metrics:article_count")) or 0,
                    "avg_tone": self._decode_float(data.get(b"metrics:avg_tone")),
                    "avg_goldstein": self._decode_float(data.get(b"metrics:avg_goldstein")),
                    "cooperation_count": self._decode_int(data.get(b"metrics:cooperation_count")) or 0,
                    "conflict_count": self._decode_int(data.get(b"metrics:conflict_count")) or 0,
                })
            return rows
        except Exception as exc:
            logger.error("HBase country_metrics scan error: %s", exc)
            return []

    def _scan_topic_metrics(self, from_dt: datetime, to_dt: datetime, topic: Optional[str] = None) -> List[Dict]:
        table = self._table("topic_metrics_by_time")
        if table is None:
            return []
        try:
            row_start = (f"{topic}_{_rev_ts(to_dt)}" if topic else _rev_ts(to_dt)).encode()
            row_stop = (f"{topic}_{_rev_ts(from_dt)}" if topic else _rev_ts(from_dt)).encode()
            rows = []
            for _, data in table.scan(row_start=row_start, row_stop=row_stop):
                rows.append({
                    "topic": self._decode(data.get(b"metrics:topic")),
                    "date": self._decode(data.get(b"metrics:date")),
                    "count": self._decode_int(data.get(b"metrics:count")) or 0,
                    "article_count": self._decode_int(data.get(b"metrics:article_count")) or 0,
                    "avg_tone": self._decode_float(data.get(b"metrics:avg_tone")),
                })
            return rows
        except Exception as exc:
            logger.error("HBase topic_metrics scan error: %s", exc)
            return []

    def _scan_category_metrics(self, from_dt: datetime, to_dt: datetime, country: Optional[str] = None) -> List[Dict]:
        table = self._table("category_metrics_by_time")
        if table is None:
            return []
        try:
            row_start = _rev_ts(to_dt).encode()
            row_stop = _rev_ts(from_dt).encode()
            rows = []
            for _, data in table.scan(row_start=row_start, row_stop=row_stop):
                rows.append({
                    "root_code": self._decode(data.get(b"metrics:root_code")),
                    "date": self._decode(data.get(b"metrics:date")),
                    "count": self._decode_int(data.get(b"metrics:count")) or 0,
                    "avg_tone": self._decode_float(data.get(b"metrics:avg_tone")),
                    "avg_goldstein": self._decode_float(data.get(b"metrics:avg_goldstein")),
                })
            return rows
        except Exception as exc:
            logger.error("HBase category_metrics scan error: %s", exc)
            return []

