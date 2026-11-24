"""
HBase service layer — all query logic for the API.
Reads real GDELT data and Spark aggregates stored in HBase via Thrift.
Handles HBase connection safely (never leaks sockets) and falls back cleanly
when HBase is offline (e.g. unit tests) or empty before initial ingestion.
"""
from __future__ import annotations

import json
import logging
import os
import sys
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Dict, Generator, List, Optional, Tuple

from src.common.cameo_mapping import CAMEO_ROOT_CATEGORIES, QUAD_CLASS_NAMES, get_category_name
from src.common.config import get_settings
from backend.services.geography_service import get_country_name

logger = logging.getLogger(__name__)
settings = get_settings()

_MAX_TS = sys.maxsize
_SALT_BUCKETS = 8

# Lightweight 10-second cache for "does HBase contain real data?"
_DATA_CHECK_CACHE: Tuple[float, bool] = (0.0, False)


def _rev_ts(dt: datetime) -> str:
    dt_utc = dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    epoch = int(dt_utc.timestamp())
    return str(_MAX_TS - epoch).zfill(19)


def _ts_from_rev(rev_str: str) -> Optional[int]:
    try:
        return _MAX_TS - int(rev_str)
    except Exception:
        return None


def _date_bounds(from_dt: datetime, to_dt: datetime) -> Tuple[str, str]:
    f_utc = from_dt.astimezone(timezone.utc) if from_dt.tzinfo else from_dt.replace(tzinfo=timezone.utc)
    t_utc = to_dt.astimezone(timezone.utc) if to_dt.tzinfo else to_dt.replace(tzinfo=timezone.utc)
    return f_utc.strftime("%Y-%m-%d"), t_utc.strftime("%Y-%m-%d")


def _empty_paginated(page: int = 1, limit: int = 50) -> Dict:
    return {
        "items": [],
        "total": 0,
        "page": page,
        "page_size": limit,
        "total_pages": 0,
        "message": "No data available for the selected time range",
    }


def _empty_list() -> Dict:
    return {"data": [], "total": 0, "message": "No data available for the selected time range"}


class HBaseService:
    """Service layer that reads live data from HBase via Thrift."""

    def __init__(self) -> None:
        self._demo = None

    def _get_demo(self):
        if self._demo is None:
            from backend.services.demo_data_service import DemoDataService
            self._demo = DemoDataService()
        return self._demo

    @contextmanager
    def _connection(self) -> Generator[Any, None, None]:
        """Open and safely close an HBase Thrift connection."""
        import happybase
        conn = happybase.Connection(
            host=settings.hbase_host,
            port=settings.hbase_thrift_port,
            timeout=8000,
        )
        conn.open()
        try:
            yield conn
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def _hbase_status(self) -> Tuple[bool, bool]:
        """Return (hbase_reachable, has_real_data) cached briefly."""
        global _DATA_CHECK_CACHE
        now = time.time()
        cached_at, cached_has = _DATA_CHECK_CACHE
        if now - cached_at < 10.0 and cached_has:
            return True, True

        try:
            with self._connection() as conn:
                tables = {t.decode() for t in conn.tables()}
                has_data = False
                if "events_by_id" in tables:
                    for _ in conn.table("events_by_id").scan(limit=1):
                        has_data = True
                        break
                if not has_data and "country_metrics_by_time" in tables:
                    for _ in conn.table("country_metrics_by_time").scan(limit=1):
                        has_data = True
                        break
                _DATA_CHECK_CACHE = (now, has_data)
                return True, has_data
        except Exception as exc:
            logger.debug("HBase check failed: %s", exc)
            return False, False

    @property
    def _use_demo(self) -> bool:
        """
        Only fall back to DemoDataService when HBase is completely unreachable
        AND demo fallback is enabled (e.g. local unit tests without Docker).
        Never override live HBase data or empty live HBase with fake demo numbers.
        """
        reachable, _ = self._hbase_status()
        if reachable:
            return False
        return os.environ.get("GDELT_OFFLINE_DEMO", "true").lower() in ("1", "true", "yes")

    @staticmethod
    def _decode(val: Optional[bytes]) -> Optional[str]:
        if val is None or val == b"":
            return None
        try:
            return val.decode("utf-8", errors="replace")
        except Exception:
            return None

    @staticmethod
    def _decode_float(val: Optional[bytes]) -> Optional[float]:
        s = HBaseService._decode(val)
        if s is None or s == "":
            return None
        try:
            return float(s)
        except ValueError:
            return None

    @staticmethod
    def _decode_int(val: Optional[bytes]) -> Optional[int]:
        s = HBaseService._decode(val)
        if s is None or s == "":
            return None
        try:
            return int(float(s))
        except ValueError:
            return None

    @staticmethod
    def _decode_bool(val: Optional[bytes]) -> bool:
        s = HBaseService._decode(val)
        return str(s).lower() in ("true", "1", "yes") if s else False

    # ------------------------------------------------------------------
    # Overview
    # ------------------------------------------------------------------
    def get_overview(self, from_dt: datetime, to_dt: datetime) -> Dict:
        if self._use_demo:
            return self._get_demo().get_overview(from_dt, to_dt)

        country_metrics = self._scan_country_metrics(from_dt, to_dt)
        category_metrics = self._scan_category_metrics(from_dt, to_dt)

        # Aggregate country rows by country_code
        by_country: Dict[str, Dict[str, Any]] = {}
        by_date: Dict[str, Dict[str, Any]] = {}
        for m in country_metrics:
            cc = m.get("country_code")
            if not cc:
                continue
            entry = by_country.setdefault(cc, {
                "country_code": cc,
                "country_name": m.get("country_name") or get_country_name(cc),
                "event_count": 0,
                "mention_count": 0,
                "article_count": 0,
                "tone_sum": 0.0,
                "tone_n": 0,
            })
            cnt = m.get("event_count", 0)
            entry["event_count"] += cnt
            entry["mention_count"] += m.get("mention_count", 0)
            entry["article_count"] += m.get("article_count", 0)
            if m.get("avg_tone") is not None:
                entry["tone_sum"] += m["avg_tone"] * max(cnt, 1)
                entry["tone_n"] += max(cnt, 1)

            d = m.get("date")
            if d:
                d_ent = by_date.setdefault(d, {"date": d, "count": 0, "tone_sum": 0.0, "gs_sum": 0.0, "n": 0})
                d_ent["count"] += cnt
                if m.get("avg_tone") is not None:
                    d_ent["tone_sum"] += m["avg_tone"] * max(cnt, 1)
                if m.get("avg_goldstein") is not None:
                    d_ent["gs_sum"] += m["avg_goldstein"] * max(cnt, 1)
                d_ent["n"] += max(cnt, 1)

        total_events = sum(v["event_count"] for v in by_country.values())
        total_mentions = sum(v["mention_count"] for v in by_country.values())
        total_articles = sum(v["article_count"] for v in by_country.values())

        tone_weighted = sum(v["tone_sum"] for v in by_country.values())
        tone_weight_n = sum(v["tone_n"] for v in by_country.values())
        avg_tone = round(tone_weighted / tone_weight_n, 3) if tone_weight_n > 0 else None

        top_countries = sorted(
            [
                {
                    "country_code": v["country_code"],
                    "country_name": v["country_name"],
                    "event_count": v["event_count"],
                    "mention_count": v["mention_count"],
                    "avg_tone": round(v["tone_sum"] / v["tone_n"], 3) if v["tone_n"] else None,
                }
                for v in by_country.values()
            ],
            key=lambda x: x["event_count"],
            reverse=True,
        )[:10]

        event_timeline = [
            {
                "date": v["date"],
                "count": v["count"],
                "avg_tone": round(v["tone_sum"] / v["n"], 2) if v["n"] else 0.0,
                "avg_goldstein": round(v["gs_sum"] / v["n"], 2) if v["n"] else 0.0,
            }
            for v in sorted(by_date.values(), key=lambda x: x["date"])
        ]

        # Build category distribution
        cat_totals: Dict[str, int] = {}
        for m in category_metrics:
            rc = m.get("root_code") or ""
            if rc:
                cat_totals[rc] = cat_totals.get(rc, 0) + m.get("count", 0)
        grand_total = sum(cat_totals.values()) or 1
        cat_dist = [
            {
                "category": rc,
                "category_name": get_category_name(rc),
                "count": cnt,
                "pct": round(cnt / grand_total * 100, 1),
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
            "active_countries": len(by_country),
            "avg_tone": avg_tone,
            "top_countries": top_countries,
            "event_timeline": event_timeline,
            "category_distribution": cat_dist,
            "cooperation_count": coop,
            "conflict_count": conf,
            "cooperation_pct": round(coop / cc_total * 100, 1) if (coop + conf) > 0 else 0.0,
            "conflict_pct": round(conf / cc_total * 100, 1) if (coop + conf) > 0 else 0.0,
            "from_dt": from_dt.isoformat(),
            "to_dt": to_dt.isoformat(),
            "data_available": total_events > 0,
        }

    # ------------------------------------------------------------------
    # Events
    # ------------------------------------------------------------------
    def _scan_country_time_events(
        self,
        from_dt: datetime,
        to_dt: datetime,
        country: Optional[str] = None,
        category: Optional[str] = None,
        tone_min: Optional[float] = None,
        tone_max: Optional[float] = None,
        max_collect: int = 2000,
    ) -> List[Tuple[int, Dict[str, Any]]]:
        """Scan events_by_country_time handling both salted country prefix and global range."""
        from_utc = from_dt.astimezone(timezone.utc) if from_dt.tzinfo else from_dt.replace(tzinfo=timezone.utc)
        to_utc = to_dt.astimezone(timezone.utc) if to_dt.tzinfo else to_dt.replace(tzinfo=timezone.utc)
        from_epoch = int(from_utc.timestamp())
        to_epoch = int(to_utc.timestamp())

        results: List[Tuple[int, Dict[str, Any]]] = []
        try:
            with self._connection() as conn:
                if b"events_by_country_time" not in set(conn.tables()):
                    return []
                table = conn.table("events_by_country_time")

                if country:
                    for bucket in range(_SALT_BUCKETS):
                        salt = format(bucket, "x")
                        row_start = f"{salt}_{country}_{_rev_ts(to_dt)}".encode()
                        row_stop = f"{salt}_{country}_{_rev_ts(from_dt)}\xff".encode()
                        for rk, data in table.scan(row_start=row_start, row_stop=row_stop):
                            ev = self._row_to_event(data)
                            if category and ev.get("EventRootCode") != category:
                                continue
                            if tone_min is not None and (ev.get("AvgTone") is None or ev["AvgTone"] < tone_min):
                                continue
                            if tone_max is not None and (ev.get("AvgTone") is None or ev["AvgTone"] > tone_max):
                                continue
                            parts = rk.decode("utf-8", errors="ignore").split("_")
                            ts = _ts_from_rev(parts[2]) if len(parts) >= 3 else 0
                            results.append((ts or 0, ev))
                            if len(results) >= max_collect:
                                break
                        if len(results) >= max_collect:
                            break
                else:
                    # Global scan across salt buckets, filtering row-key timestamp
                    seen = 0
                    for rk, data in table.scan(limit=max_collect * 4):
                        seen += 1
                        parts = rk.decode("utf-8", errors="ignore").split("_")
                        ts = _ts_from_rev(parts[2]) if len(parts) >= 3 else None
                        if ts is not None and not (from_epoch <= ts <= to_epoch):
                            continue
                        ev = self._row_to_event(data)
                        if category and ev.get("EventRootCode") != category:
                            continue
                        if tone_min is not None and (ev.get("AvgTone") is None or ev["AvgTone"] < tone_min):
                            continue
                        if tone_max is not None and (ev.get("AvgTone") is None or ev["AvgTone"] > tone_max):
                            continue
                        results.append((ts or 0, ev))
                        if len(results) >= max_collect:
                            break
        except Exception as exc:
            logger.error("HBase events scan error: %s", exc)
            return []

        results.sort(key=lambda x: (x[0], x[1].get("SQLDATE") or 0), reverse=True)
        return results

    def get_events(
        self,
        from_dt: datetime,
        to_dt: datetime,
        country: Optional[str] = None,
        topic: Optional[str] = None,
        category: Optional[str] = None,
        tone_min: Optional[float] = None,
        tone_max: Optional[float] = None,
        page: int = 1,
        limit: int = 50,
    ) -> Dict:
        if self._use_demo:
            return self._get_demo().get_events(
                from_dt, to_dt, country=country, category=category, page=page, limit=limit
            )
        matched = self._scan_country_time_events(
            from_dt, to_dt,
            country=country, category=category,
            tone_min=tone_min, tone_max=tone_max,
            max_collect=min(max(page * limit + 500, 1000), 3000),
        )
        if not matched:
            return _empty_paginated(page, limit)

        total = len(matched)
        offset = (page - 1) * limit
        page_items = [ev for _, ev in matched[offset: offset + limit]]
        return {
            "items": page_items,
            "total": total,
            "page": page,
            "page_size": limit,
            "total_pages": max(1, (total + limit - 1) // limit),
        }

    def get_event_by_id(self, event_id: int) -> Optional[Dict]:
        if self._use_demo:
            return self._get_demo().get_event_by_id(event_id)
        try:
            with self._connection() as conn:
                if b"events_by_id" not in set(conn.tables()):
                    return None
                row = conn.table("events_by_id").row(str(event_id).encode())
                return self._row_to_event(row) if row else None
        except Exception as exc:
            logger.error("HBase get_event error: %s", exc)
            return None

    def get_event_mentions(self, event_id: int, limit: int = 50) -> Dict:
        if self._use_demo:
            return self._get_demo().get_event_mentions(event_id, limit)
        ev = self.get_event_by_id(event_id)
        if not ev:
            return {"mentions": [], "total": 0}
        return {
            "mentions": [
                {
                    "MentionID": f"{event_id}-0",
                    "EventID": event_id,
                    "MentionSourceName": (ev.get("SOURCEURL") or "").split("/")[2] if ev.get("SOURCEURL") and "/" in ev.get("SOURCEURL", "") else "source",
                    "MentionDocTone": ev.get("AvgTone"),
                    "MentionTimeDate": str(ev.get("SQLDATE") or ""),
                }
            ],
            "total": ev.get("NumMentions") or 1,
        }

    def _row_to_event(self, data: Dict[bytes, bytes]) -> Dict:
        """Decode an event row whether stored in `event:` (events_by_id) or `data:` (events_by_country_time)."""
        def g(k_ev: str, k_data: str) -> Optional[str]:
            return self._decode(data.get(k_ev.encode()) or data.get(k_data.encode()))

        def gf(k_ev: str, k_data: str) -> Optional[float]:
            val = data.get(k_ev.encode())
            if val is None:
                val = data.get(k_data.encode())
            return self._decode_float(val)

        def gi(k_ev: str, k_data: str) -> Optional[int]:
            val = data.get(k_ev.encode())
            if val is None:
                val = data.get(k_data.encode())
            return self._decode_int(val)

        rc = g("event:EventRootCode", "data:event_root_code") or ""
        qc = gi("event:QuadClass", "data:quad_class")
        sqldate = gi("event:SQLDATE", "data:date")
        date_str = str(sqldate) if sqldate else ""
        iso_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}" if len(date_str) == 8 else date_str

        return {
            "GlobalEventID": gi("event:GlobalEventID", "data:event_id"),
            "SQLDATE": sqldate,
            "EventDate": iso_date,
            "Actor1Name": g("event:Actor1Name", "data:actor1_name"),
            "Actor1CountryCode": g("event:Actor1CountryCode", "data:actor1_country"),
            "Actor2Name": g("event:Actor2Name", "data:actor2_name"),
            "Actor2CountryCode": g("event:Actor2CountryCode", "data:actor2_country"),
            "ActionGeo_CountryCode": g("event:ActionGeo_CountryCode", "data:country"),
            "ActionGeo_FullName": g("event:ActionGeo_FullName", "data:full_name"),
            "ActionGeo_Lat": gf("event:ActionGeo_Lat", "data:lat"),
            "ActionGeo_Long": gf("event:ActionGeo_Long", "data:lon"),
            "ActionGeo_Type": gi("event:ActionGeo_Type", "data:geo_type"),
            "EventCode": g("event:EventCode", "data:event_code"),
            "EventRootCode": rc,
            "EventCategoryName": get_category_name(rc),
            "QuadClass": qc,
            "QuadClassName": QUAD_CLASS_NAMES.get(qc or 0, "Unknown"),
            "GoldsteinScale": gf("event:GoldsteinScale", "data:goldstein"),
            "NumMentions": gi("event:NumMentions", "data:num_mentions"),
            "NumArticles": gi("event:NumArticles", "data:num_articles"),
            "AvgTone": gf("event:AvgTone", "data:avg_tone"),
            "SOURCEURL": g("event:SOURCEURL", "data:source_url"),
            "IsConflict": CAMEO_ROOT_CATEGORIES.get(rc, ("", False))[1],
        }

    # ------------------------------------------------------------------
    # Topics
    # ------------------------------------------------------------------
    def get_topics(
        self,
        from_dt: datetime,
        to_dt: datetime,
        country: Optional[str] = None,
        limit: int = 20,
    ) -> Dict:
        if self._use_demo:
            return self._get_demo().get_topics(from_dt, to_dt, country=country)
        rows = self._scan_topic_metrics(from_dt, to_dt)
        agg: Dict[str, Dict] = {}
        for r in rows:
            t = r.get("topic") or ""
            if not t:
                continue
            dist = r.get("country_distribution") or {}
            if country:
                c_cnt = dist.get(country.upper(), 0)
                if c_cnt <= 0:
                    continue
                cnt = c_cnt
            else:
                cnt = r.get("count", 0)
            ent = agg.setdefault(t, {"topic": t, "count": 0, "article_count": 0, "tone_sum": 0.0, "tone_n": 0})
            ent["count"] += cnt
            ent["article_count"] += r.get("article_count", 0)
            if r.get("avg_tone") is not None:
                ent["tone_sum"] += r["avg_tone"] * max(cnt, 1)
                ent["tone_n"] += max(cnt, 1)

        topics = [
            {
                "topic": t,
                "count": v["count"],
                "article_count": v["article_count"],
                "avg_tone": round(v["tone_sum"] / v["tone_n"], 3) if v["tone_n"] else None,
            }
            for t, v in agg.items()
        ]
        topics.sort(key=lambda x: x["count"], reverse=True)
        return {"data": topics[:limit], "total": len(topics)}

    def get_topic_trend(self, topic: str, from_dt: datetime, to_dt: datetime) -> Dict:
        if self._use_demo:
            return self._get_demo().get_topic_trend(topic, from_dt, to_dt)
        rows = self._scan_topic_metrics(from_dt, to_dt, topic=topic)
        by_date: Dict[str, Dict[str, Any]] = {}
        for r in rows:
            d = r.get("date")
            if not d:
                continue
            ent = by_date.setdefault(d, {"date": d, "count": 0, "tone_sum": 0.0, "tone_n": 0})
            cnt = r.get("count", 0)
            ent["count"] += cnt
            if r.get("avg_tone") is not None:
                ent["tone_sum"] += r["avg_tone"] * max(cnt, 1)
                ent["tone_n"] += max(cnt, 1)
        series = [
            {
                "date": v["date"],
                "count": v["count"],
                "avg_tone": round(v["tone_sum"] / v["tone_n"], 2) if v["tone_n"] else None,
            }
            for v in sorted(by_date.values(), key=lambda x: x["date"])
        ]
        return {"topic": topic, "trend": series}

    def get_topic_countries(self, topic: str, from_dt: datetime, to_dt: datetime) -> Dict:
        if self._use_demo:
            return self._get_demo().get_topic_countries(topic, from_dt, to_dt)
        rows = self._scan_topic_metrics(from_dt, to_dt, topic=topic)
        country_counts: Dict[str, int] = {}
        for r in rows:
            for cc, cnt in (r.get("country_distribution") or {}).items():
                country_counts[cc] = country_counts.get(cc, 0) + int(cnt)
        result = sorted(
            [{"country_code": cc, "country_name": get_country_name(cc), "count": cnt} for cc, cnt in country_counts.items()],
            key=lambda x: x["count"],
            reverse=True,
        )
        return {"topic": topic, "countries": result}

    # ------------------------------------------------------------------
    # Countries
    # ------------------------------------------------------------------
    def get_country_overview(self, country_code: str, from_dt: datetime, to_dt: datetime) -> Dict:
        if self._use_demo:
            return self._get_demo().get_country_overview(country_code, from_dt, to_dt)
        metrics = self._scan_country_metrics(from_dt, to_dt, country=country_code)
        if not metrics:
            return {
                "country_code": country_code,
                "country_name": get_country_name(country_code),
                "data_available": False,
                "message": "No data for this country in the selected range",
            }
        total_events = sum(m.get("event_count", 0) for m in metrics)
        total_mentions = sum(m.get("mention_count", 0) for m in metrics)

        tone_sum = sum((m["avg_tone"] or 0.0) * max(m.get("event_count", 1), 1) for m in metrics if m.get("avg_tone") is not None)
        tone_n = sum(max(m.get("event_count", 1), 1) for m in metrics if m.get("avg_tone") is not None)
        gs_sum = sum((m["avg_goldstein"] or 0.0) * max(m.get("event_count", 1), 1) for m in metrics if m.get("avg_goldstein") is not None)
        gs_n = sum(max(m.get("event_count", 1), 1) for m in metrics if m.get("avg_goldstein") is not None)

        coop = sum(m.get("cooperation_count", 0) for m in metrics)
        conf = sum(m.get("conflict_count", 0) for m in metrics)

        by_date: Dict[str, Dict[str, Any]] = {}
        for m in metrics:
            d = m.get("date")
            if not d:
                continue
            ent = by_date.setdefault(d, {"date": d, "event_count": 0, "tone_sum": 0.0, "n": 0})
            c = m.get("event_count", 0)
            ent["event_count"] += c
            if m.get("avg_tone") is not None:
                ent["tone_sum"] += m["avg_tone"] * max(c, 1)
                ent["n"] += max(c, 1)
        timeline = [
            {
                "date": v["date"],
                "event_count": v["event_count"],
                "avg_tone": round(v["tone_sum"] / v["n"], 2) if v["n"] else None,
            }
            for v in sorted(by_date.values(), key=lambda x: x["date"])
        ]

        return {
            "country_code": country_code,
            "country_name": get_country_name(country_code),
            "total_events": total_events,
            "total_mentions": total_mentions,
            "avg_tone": round(tone_sum / tone_n, 3) if tone_n else None,
            "avg_goldstein": round(gs_sum / gs_n, 3) if gs_n else None,
            "cooperation_count": coop,
            "conflict_count": conf,
            "event_timeline": timeline,
            "data_available": total_events > 0,
        }

    def get_country_regions(self, country_code: str, from_dt: datetime, to_dt: datetime) -> Dict:
        if self._use_demo:
            return self._get_demo().get_country_regions(country_code, from_dt, to_dt)
        matched = self._scan_country_time_events(from_dt, to_dt, country=country_code, max_collect=500)
        by_region: Dict[str, Dict[str, Any]] = {}
        for _, ev in matched:
            name = ev.get("ActionGeo_FullName") or "Unknown"
            ent = by_region.setdefault(name, {"region_name": name, "event_count": 0, "tone_sum": 0.0, "tone_n": 0})
            ent["event_count"] += 1
            if ev.get("AvgTone") is not None:
                ent["tone_sum"] += ev["AvgTone"]
                ent["tone_n"] += 1
        regions = sorted(
            [
                {
                    "region_name": v["region_name"],
                    "event_count": v["event_count"],
                    "avg_tone": round(v["tone_sum"] / v["tone_n"], 2) if v["tone_n"] else None,
                }
                for v in by_region.values()
            ],
            key=lambda x: x["event_count"],
            reverse=True,
        )[:15]
        return {"country_code": country_code, "regions": regions}

    def compare_countries(self, codes: List[str], from_dt: datetime, to_dt: datetime) -> Dict:
        if self._use_demo:
            return self._get_demo().compare_countries(codes, from_dt, to_dt)
        result = [self.get_country_overview(code, from_dt, to_dt) for code in codes]
        return {"countries": result, "from_dt": from_dt.isoformat(), "to_dt": to_dt.isoformat()}

    # ------------------------------------------------------------------
    # Analytics
    # ------------------------------------------------------------------
    def get_category_metrics(self, from_dt: datetime, to_dt: datetime, country: Optional[str] = None) -> Dict:
        if self._use_demo:
            return self._get_demo().get_category_metrics(from_dt, to_dt, country=country)

        agg: Dict[str, Dict] = {}
        if country:
            matched = self._scan_country_time_events(from_dt, to_dt, country=country, max_collect=1500)
            for _, ev in matched:
                rc = ev.get("EventRootCode") or ""
                if not rc:
                    continue
                ent = agg.setdefault(rc, {
                    "root_code": rc,
                    "category_name": get_category_name(rc),
                    "count": 0, "tone_sum": 0.0, "tone_n": 0,
                    "goldstein_sum": 0.0, "goldstein_n": 0,
                    "is_conflict": CAMEO_ROOT_CATEGORIES.get(rc, ("", False))[1],
                })
                ent["count"] += 1
                if ev.get("AvgTone") is not None:
                    ent["tone_sum"] += ev["AvgTone"]
                    ent["tone_n"] += 1
                if ev.get("GoldsteinScale") is not None:
                    ent["goldstein_sum"] += ev["GoldsteinScale"]
                    ent["goldstein_n"] += 1
        else:
            rows = self._scan_category_metrics(from_dt, to_dt)
            for r in rows:
                rc = r.get("root_code") or ""
                if not rc:
                    continue
                cnt = r.get("count", 0)
                ent = agg.setdefault(rc, {
                    "root_code": rc,
                    "category_name": get_category_name(rc),
                    "count": 0, "tone_sum": 0.0, "tone_n": 0,
                    "goldstein_sum": 0.0, "goldstein_n": 0,
                    "is_conflict": CAMEO_ROOT_CATEGORIES.get(rc, ("", False))[1],
                })
                ent["count"] += cnt
                if r.get("avg_tone") is not None:
                    ent["tone_sum"] += r["avg_tone"] * max(cnt, 1)
                    ent["tone_n"] += max(cnt, 1)
                if r.get("avg_goldstein") is not None:
                    ent["goldstein_sum"] += r["avg_goldstein"] * max(cnt, 1)
                    ent["goldstein_n"] += max(cnt, 1)

        total = sum(v["count"] for v in agg.values()) or 1
        result = [
            {
                "root_code": rc,
                "category": rc,
                "category_name": v["category_name"],
                "count": v["count"],
                "pct": round(v["count"] / total * 100, 1),
                "avg_tone": round(v["tone_sum"] / v["tone_n"], 3) if v["tone_n"] else None,
                "avg_goldstein": round(v["goldstein_sum"] / v["goldstein_n"], 3) if v["goldstein_n"] else None,
                "is_conflict": v["is_conflict"],
            }
            for rc, v in sorted(agg.items(), key=lambda x: x[1]["count"], reverse=True)
        ]
        return {"data": result, "total": sum(v["count"] for v in agg.values())}

    def get_tone_metrics(self, from_dt: datetime, to_dt: datetime, country: Optional[str] = None, topic: Optional[str] = None) -> Dict:
        if self._use_demo:
            return self._get_demo().get_tone_metrics(from_dt, to_dt, country=country, topic=topic)
        metrics = self._scan_country_metrics(from_dt, to_dt, country=country)
        by_date: Dict[str, Dict[str, Any]] = {}
        for m in metrics:
            d = m.get("date")
            if not d or m.get("avg_tone") is None:
                continue
            cnt = max(m.get("event_count", 1), 1)
            ent = by_date.setdefault(d, {"date": d, "tone_sum": 0.0, "count": 0})
            ent["tone_sum"] += m["avg_tone"] * cnt
            ent["count"] += cnt

        series = [
            {
                "date": v["date"],
                "avg_tone": round(v["tone_sum"] / v["count"], 3),
                "count": v["count"],
            }
            for v in sorted(by_date.values(), key=lambda x: x["date"])
        ]
        total_sum = sum(v["tone_sum"] for v in by_date.values())
        total_cnt = sum(v["count"] for v in by_date.values())
        return {
            "trend": series,
            "overall_avg_tone": round(total_sum / total_cnt, 3) if total_cnt > 0 else None,
            "country": country,
            "topic": topic,
            "note": "Tone reflects textual reporting tone (GDELT AvgTone), not factual sentiment.",
        }

    def get_cooperation_conflict(self, from_dt: datetime, to_dt: datetime, country: Optional[str] = None) -> Dict:
        if self._use_demo:
            return self._get_demo().get_cooperation_conflict(from_dt, to_dt, country=country)
        metrics = self._scan_country_metrics(from_dt, to_dt, country=country)
        coop = sum(m.get("cooperation_count", 0) for m in metrics)
        conf = sum(m.get("conflict_count", 0) for m in metrics)
        total = (coop + conf) or 1
        return {
            "cooperation_count": coop,
            "conflict_count": conf,
            "cooperation_pct": round(coop / total * 100, 1) if (coop + conf) > 0 else 0.0,
            "conflict_pct": round(conf / total * 100, 1) if (coop + conf) > 0 else 0.0,
            "country": country,
        }

    def get_anomalies(self, from_dt: datetime, to_dt: datetime, country: Optional[str] = None, min_z_score: float = 2.5) -> Dict:
        if self._use_demo:
            return self._get_demo().get_anomalies(from_dt, to_dt, country=country, min_z_score=min_z_score)
        try:
            with self._connection() as conn:
                if b"anomalies_by_time" not in set(conn.tables()):
                    return _empty_list()
                row_start = _rev_ts(to_dt).encode()
                row_stop = f"{_rev_ts(from_dt)}\xff".encode()
                items = []
                for _, data in conn.table("anomalies_by_time").scan(row_start=row_start, row_stop=row_stop):
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
    def get_map_events(
        self,
        from_dt: datetime,
        to_dt: datetime,
        country: Optional[str] = None,
        topic: Optional[str] = None,
        category: Optional[str] = None,
        max_points: int = 2000,
    ) -> Dict:
        if self._use_demo:
            return self._get_demo().get_map_events(from_dt, to_dt, category=category, limit=max_points)
        events_resp = self.get_events(
            from_dt, to_dt, country=country, category=category, page=1, limit=max_points
        )
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
            for e in items
            if e.get("ActionGeo_Lat") is not None and e.get("ActionGeo_Long") is not None
        ]
        no_coords = len(items) - len(markers)
        return {"markers": markers, "total": len(markers), "no_coordinates_count": no_coords}

    def get_map_countries(self, from_dt: datetime, to_dt: datetime) -> Dict:
        if self._use_demo:
            return self._get_demo().get_map_countries(from_dt, to_dt)
        metrics = self._scan_country_metrics(from_dt, to_dt)
        agg: Dict[str, int] = {}
        for m in metrics:
            cc = m.get("country_code")
            if cc:
                agg[cc] = agg.get(cc, 0) + m.get("event_count", 0)
        max_val = max(agg.values(), default=1)
        result = [
            {
                "country_code": cc,
                "country_name": get_country_name(cc),
                "event_count": cnt,
                "value": cnt,
                "normalized": round(cnt / max_val, 4),
            }
            for cc, cnt in sorted(agg.items(), key=lambda x: x[1], reverse=True)
        ]
        return {"data": result, "total": len(result)}

    # ------------------------------------------------------------------
    # Search
    # ------------------------------------------------------------------
    def search_events(
        self, q: str, from_dt: datetime, to_dt: datetime, page: int = 1, limit: int = 50
    ) -> Dict:
        if self._use_demo:
            return self._get_demo().search_events(q, from_dt, to_dt, page=page, limit=limit)
        matched = self._scan_country_time_events(from_dt, to_dt, max_collect=2000)
        q_lower = q.lower()
        filtered = [
            ev
            for _, ev in matched
            if q_lower in str(ev.get("Actor1Name") or "").lower()
            or q_lower in str(ev.get("Actor2Name") or "").lower()
            or q_lower in str(ev.get("ActionGeo_FullName") or "").lower()
            or q_lower in str(ev.get("ActionGeo_CountryCode") or "").lower()
            or q_lower in str(ev.get("EventCategoryName") or "").lower()
            or q_lower in str(ev.get("SOURCEURL") or "").lower()
        ]
        total = len(filtered)
        offset = (page - 1) * limit
        return {
            "items": filtered[offset: offset + limit],
            "total": total,
            "query": q,
            "page": page,
            "page_size": limit,
            "total_pages": max(1, (total + limit - 1) // limit) if total else 0,
        }

    # ------------------------------------------------------------------
    # Region
    # ------------------------------------------------------------------
    def get_region_overview(self, region_id: str, from_dt: datetime, to_dt: datetime) -> Dict:
        if self._use_demo:
            return self._get_demo().get_region_overview(region_id, from_dt, to_dt)
        return {
            "region_id": region_id,
            "data_available": False,
            "note": "Region-level aggregation requires ADM1 boundary data integration.",
        }

    # ------------------------------------------------------------------
    # Private HBase scan helpers
    # ------------------------------------------------------------------
    def _scan_country_metrics(
        self, from_dt: datetime, to_dt: datetime, country: Optional[str] = None
    ) -> List[Dict]:
        from_date, to_date = _date_bounds(from_dt, to_dt)
        try:
            with self._connection() as conn:
                if b"country_metrics_by_time" not in set(conn.tables()):
                    return []
                table = conn.table("country_metrics_by_time")
                if country:
                    row_start = f"{country}_{_rev_ts(to_dt)}".encode()
                    row_stop = f"{country}_{_rev_ts(from_dt)}\xff".encode()
                    scanner = table.scan(row_start=row_start, row_stop=row_stop)
                else:
                    scanner = table.scan()

                rows = []
                for rk, data in scanner:
                    d_str = self._decode(data.get(b"metrics:date"))
                    if d_str and not (from_date <= d_str <= to_date):
                        continue
                    cc = self._decode(data.get(b"metrics:country_code"))
                    if not cc:
                        rk_s = rk.decode("utf-8", errors="ignore")
                        cc = rk_s.split("_")[0] if "_" in rk_s else None
                    rows.append({
                        "country_code": cc,
                        "country_name": self._decode(data.get(b"metrics:country_name")) or (get_country_name(cc) if cc else None),
                        "date": d_str,
                        "hour": self._decode_int(data.get(b"metrics:hour")),
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

    def _scan_topic_metrics(
        self, from_dt: datetime, to_dt: datetime, topic: Optional[str] = None
    ) -> List[Dict]:
        from_date, to_date = _date_bounds(from_dt, to_dt)
        try:
            with self._connection() as conn:
                if b"topic_metrics_by_time" not in set(conn.tables()):
                    return []
                table = conn.table("topic_metrics_by_time")
                if topic:
                    row_start = f"{topic}_{_rev_ts(to_dt)}".encode()
                    row_stop = f"{topic}_{_rev_ts(from_dt)}\xff".encode()
                    scanner = table.scan(row_start=row_start, row_stop=row_stop)
                else:
                    scanner = table.scan()

                rows = []
                for rk, data in scanner:
                    d_str = self._decode(data.get(b"metrics:date"))
                    if d_str and not (from_date <= d_str <= to_date):
                        continue
                    t_name = self._decode(data.get(b"metrics:topic"))
                    if not t_name:
                        rk_s = rk.decode("utf-8", errors="ignore")
                        t_name = rk_s.rsplit("_", 1)[0] if "_" in rk_s else None
                    dist_raw = self._decode(data.get(b"metrics:country_distribution"))
                    try:
                        dist = json.loads(dist_raw) if dist_raw else {}
                    except Exception:
                        dist = {}
                    rows.append({
                        "topic": t_name,
                        "date": d_str,
                        "count": self._decode_int(data.get(b"metrics:count")) or 0,
                        "article_count": self._decode_int(data.get(b"metrics:article_count")) or 0,
                        "avg_tone": self._decode_float(data.get(b"metrics:avg_tone")),
                        "country_distribution": dist,
                    })
                return rows
        except Exception as exc:
            logger.error("HBase topic_metrics scan error: %s", exc)
            return []

    def _scan_category_metrics(
        self, from_dt: datetime, to_dt: datetime, country: Optional[str] = None
    ) -> List[Dict]:
        from_date, to_date = _date_bounds(from_dt, to_dt)
        try:
            with self._connection() as conn:
                if b"category_metrics_by_time" not in set(conn.tables()):
                    return []
                table = conn.table("category_metrics_by_time")
                rows = []
                for rk, data in table.scan():
                    d_str = self._decode(data.get(b"metrics:date"))
                    if d_str and not (from_date <= d_str <= to_date):
                        continue
                    rc = self._decode(data.get(b"metrics:root_code"))
                    if not rc:
                        rk_s = rk.decode("utf-8", errors="ignore")
                        rc = rk_s.split("_")[0] if "_" in rk_s else None
                    rows.append({
                        "root_code": rc,
                        "date": d_str,
                        "count": self._decode_int(data.get(b"metrics:count")) or 0,
                        "avg_tone": self._decode_float(data.get(b"metrics:avg_tone")),
                        "avg_goldstein": self._decode_float(data.get(b"metrics:avg_goldstein")),
                    })
                return rows
        except Exception as exc:
            logger.error("HBase category_metrics scan error: %s", exc)
            return []
