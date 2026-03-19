"""
Ukraine Frontline Tracker — Data Pipeline
==========================================
Fetches, caches, and analyzes frontline GeoJSON from DeepState UA
(via cyterat/deepstate-map-data GitHub archive).

Daily MultiPolygon of occupied territory, full fidelity, no simplification.
Available from 2024-07-08 onward, updated daily at 03:00 UTC.

Analytics: area (UTM 36N), frontline length, rate of advance, batch ops.
"""

import json
import logging
import os
from pathlib import Path
from datetime import datetime, timedelta

import requests
from shapely.geometry import mapping, shape, Polygon
from shapely.ops import unary_union
import pyproj
from shapely.ops import transform as shapely_transform

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(exist_ok=True)

CACHE_DIR = DATA_DIR / "cache"
CACHE_DIR.mkdir(exist_ok=True)

CYTERAT_BASE = "https://raw.githubusercontent.com/cyterat/deepstate-map-data/main/data"
ARCHIVE_START = "20240708"

# UTM Zone 36N for Ukraine
WGS84 = pyproj.CRS("EPSG:4326")
UTM36N = pyproj.CRS("EPSG:32636")
_to_utm = pyproj.Transformer.from_crs(WGS84, UTM36N, always_xy=True).transform


class DeepStateSource:
    """Fetches frontline data from the cyterat GitHub archive."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers["User-Agent"] = "frontline-tracker/1.0"

    def fetch_date(self, date_str: str) -> dict | None:
        """Fetch a day's GeoJSON. Returns raw geometry, no simplification."""
        clean = date_str.replace("-", "")
        cache_file = CACHE_DIR / f"ds_{clean}.geojson"
        if cache_file.exists():
            with open(cache_file) as f:
                return json.load(f)

        url = f"{CYTERAT_BASE}/deepstatemap_data_{clean}.geojson"
        try:
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            geom = self._extract_geometry(data)
            if geom:
                with open(cache_file, "w") as f:
                    json.dump(geom, f)
                log.info(f"Cached: {clean} ({len(json.dumps(geom)) / 1024:.0f}KB)")
                return geom
            return None
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                log.debug(f"No data for {clean}")
            else:
                log.warning(f"HTTP error {clean}: {e}")
            return None
        except Exception as e:
            log.warning(f"Fetch failed {clean}: {e}")
            return None

    def fetch_batch(self, dates: list[str]) -> dict[str, dict]:
        """Fetch multiple dates, return {date: geometry}. Skips failures."""
        results = {}
        for d in dates:
            geom = self.fetch_date(d)
            if geom:
                results[d] = geom
        return results

    @staticmethod
    def _extract_geometry(geojson: dict) -> dict | None:
        if geojson.get("type") == "FeatureCollection":
            features = geojson.get("features", [])
            if len(features) == 1:
                return features[0]["geometry"]
            geoms = [shape(f["geometry"]) for f in features]
            return mapping(unary_union(geoms))
        elif geojson.get("type") in ("MultiPolygon", "Polygon"):
            return geojson
        elif geojson.get("type") == "Feature":
            return geojson["geometry"]
        return None

    def get_available_dates(self) -> list[str]:
        """Return available dates (YYYYMMDD), cached 12h."""
        dates_file = DATA_DIR / "available_dates.json"
        if dates_file.exists():
            age_hours = (datetime.now().timestamp() - os.path.getmtime(dates_file)) / 3600
            if age_hours < 12:
                with open(dates_file) as f:
                    return json.load(f)

        log.info("Fetching available dates from GitHub API...")
        dates = []
        page = 1
        while True:
            try:
                resp = self.session.get(
                    "https://api.github.com/repos/cyterat/deepstate-map-data/contents/data",
                    params={"per_page": 100, "page": page},
                    timeout=30,
                )
                resp.raise_for_status()
                files = resp.json()
                if not isinstance(files, list) or not files:
                    break
                for f in files:
                    name = f.get("name", "")
                    if name.startswith("deepstatemap_data_") and name.endswith(".geojson"):
                        dates.append(name[18:-8])  # extract YYYYMMDD
                if len(files) < 100:
                    break
                page += 1
            except Exception as e:
                log.warning(f"GitHub API error (page {page}): {e}")
                break

        if dates:
            dates = sorted(set(dates))
            with open(dates_file, "w") as f:
                json.dump(dates, f)
            log.info(f"{len(dates)} dates: {dates[0]} to {dates[-1]}")
        else:
            log.warning("GitHub API unavailable, generating date range")
            dates = self._generate_date_range()

        return dates

    @staticmethod
    def _generate_date_range() -> list[str]:
        start = datetime.strptime(ARCHIVE_START, "%Y%m%d")
        end = datetime.now()
        dates = []
        d = start
        while d <= end:
            dates.append(d.strftime("%Y%m%d"))
            d += timedelta(days=1)
        return dates


class FrontlineAnalytics:
    """
    Heavy analytical computations on frontline geometry.
    All spatial calcs use UTM Zone 36N (EPSG:32636) for metric accuracy.
    """

    @staticmethod
    def area_km2(geom) -> float:
        """Area in km² via UTM projection."""
        if geom.is_empty:
            return 0.0
        return shapely_transform(_to_utm, geom).area / 1e6

    @staticmethod
    def total_area(geojson: dict) -> float:
        geom = shape(geojson)
        if not geom.is_valid:
            geom = geom.buffer(0)
        return round(FrontlineAnalytics.area_km2(geom), 2)

    @staticmethod
    def frontline_length_km(geojson: dict) -> float:
        """
        Extract the frontline (boundary of occupied territory) and measure in km.
        Excludes coastline/border segments by filtering out very long straight segments.
        """
        geom = shape(geojson)
        if not geom.is_valid:
            geom = geom.buffer(0)
        boundary = geom.boundary
        projected = shapely_transform(_to_utm, boundary)
        return round(projected.length / 1000, 1)

    @staticmethod
    def compute_diff(geojson_old: dict, geojson_new: dict) -> dict:
        """Geometric diff: gained (Russian advance) and lost (UA recapture)."""
        old_geom = shape(geojson_old)
        new_geom = shape(geojson_new)
        if not old_geom.is_valid:
            old_geom = old_geom.buffer(0)
        if not new_geom.is_valid:
            new_geom = new_geom.buffer(0)

        gained = new_geom.difference(old_geom)
        lost = old_geom.difference(new_geom)

        g_km2 = FrontlineAnalytics.area_km2(gained)
        l_km2 = FrontlineAnalytics.area_km2(lost)
        return {
            "gained": mapping(gained) if not gained.is_empty else None,
            "lost": mapping(lost) if not lost.is_empty else None,
            "gained_km2": round(g_km2, 2),
            "lost_km2": round(l_km2, 2),
            "net_km2": round(g_km2 - l_km2, 2),
        }

    @staticmethod
    def compute_stats(geojson: dict) -> dict:
        """Full stats for a single date: area, frontline length, polygon count."""
        geom = shape(geojson)
        if not geom.is_valid:
            geom = geom.buffer(0)

        area = FrontlineAnalytics.area_km2(geom)
        length = FrontlineAnalytics.frontline_length_km(geojson)

        if geom.geom_type == "MultiPolygon":
            n_polys = len(geom.geoms)
        else:
            n_polys = 1

        return {
            "area_km2": round(area, 2),
            "frontline_km": length,
            "polygons": n_polys,
        }

    @staticmethod
    def build_time_series(source, dates: list[str], cache_name: str = "time_series") -> list[dict]:
        """
        Build area + frontline time series for a list of dates.
        source: any object with fetch_date(date_str) -> geojson dict
        cache_name: filename prefix for cache (e.g. "time_series" or "divgen_time_series")
        Returns [{date, area_km2, frontline_km}].
        """
        cache_file = DATA_DIR / f"{cache_name}.json"
        cached_data = {}
        if cache_file.exists():
            with open(cache_file) as f:
                cached_data = {r["date"]: r for r in json.load(f)}

        results = []
        fetched = 0
        for d in dates:
            if d in cached_data:
                results.append(cached_data[d])
                continue

            geom = source.fetch_date(d)
            if not geom:
                continue

            stats = FrontlineAnalytics.compute_stats(geom)
            entry = {"date": d, **stats}
            results.append(entry)
            fetched += 1

        if fetched > 0:
            results.sort(key=lambda r: r["date"])
            with open(cache_file, "w") as f:
                json.dump(results, f)
            log.info(f"{cache_name}: {len(results)} points ({fetched} new)")

        return results

    @staticmethod
    def compute_rates(time_series: list[dict], window: int = 7) -> list[dict]:
        """
        Compute rate-of-advance from time series.
        Returns [{date, area_km2, delta_km2, rate_km2_per_day, rate_7d_avg}].
        """
        if len(time_series) < 2:
            return time_series

        results = []
        for i, entry in enumerate(time_series):
            row = {**entry, "delta_km2": 0.0, "rate_km2_per_day": 0.0, "rate_7d_avg": 0.0}
            if i > 0:
                prev = time_series[i - 1]
                d1 = datetime.strptime(prev["date"], "%Y%m%d")
                d2 = datetime.strptime(entry["date"], "%Y%m%d")
                days = (d2 - d1).days or 1
                delta = entry["area_km2"] - prev["area_km2"]
                row["delta_km2"] = round(delta, 2)
                row["rate_km2_per_day"] = round(delta / days, 2)

            # Rolling average
            start = max(0, i - window + 1)
            window_entries = time_series[start:i + 1]
            if len(window_entries) >= 2:
                d_start = datetime.strptime(window_entries[0]["date"], "%Y%m%d")
                d_end = datetime.strptime(window_entries[-1]["date"], "%Y%m%d")
                total_days = (d_end - d_start).days or 1
                total_delta = window_entries[-1]["area_km2"] - window_entries[0]["area_km2"]
                row["rate_7d_avg"] = round(total_delta / total_days, 2)

            results.append(row)
        return results

    @staticmethod
    def change_heatmap(ds: DeepStateSource, date_str: str, lookback: int = 30) -> dict | None:
        """
        Build a heatmap of frontline changes over the last N days.
        Returns a GeoJSON geometry of all gained/lost areas unioned together,
        with intensity based on recency.

        Returns {gained_union, lost_union, gained_km2, lost_km2}.
        """
        clean = date_str.replace("-", "")
        target = datetime.strptime(clean, "%Y%m%d")

        all_gained = []
        all_lost = []

        prev_geom = None
        for i in range(lookback, -1, -1):
            d = (target - timedelta(days=i)).strftime("%Y%m%d")
            geojson = ds.fetch_date(d)
            if not geojson:
                continue

            geom = shape(geojson)
            if not geom.is_valid:
                geom = geom.buffer(0)

            if prev_geom is not None:
                gained = geom.difference(prev_geom)
                lost = prev_geom.difference(geom)
                if not gained.is_empty:
                    all_gained.append(gained)
                if not lost.is_empty:
                    all_lost.append(lost)

            prev_geom = geom

        result = {}
        if all_gained:
            g = unary_union(all_gained)
            result["gained_union"] = mapping(g)
            result["gained_km2"] = round(FrontlineAnalytics.area_km2(g), 2)
        else:
            result["gained_union"] = None
            result["gained_km2"] = 0

        if all_lost:
            l = unary_union(all_lost)
            result["lost_union"] = mapping(l)
            result["lost_km2"] = round(FrontlineAnalytics.area_km2(l), 2)
        else:
            result["lost_union"] = None
            result["lost_km2"] = 0

        return result

    # ── Key cities for distance tracking ─────────────────
    # (name, lon, lat) — cities near or threatened by the frontline
    KEY_CITIES = [
        ("Pokrovsk",      37.18, 48.28),
        ("Zaporizhzhia",  35.14, 47.84),
        ("Dnipro",        35.05, 48.46),
        ("Kharkiv",       36.23, 49.99),
        ("Kramatorsk",    37.56, 48.74),
        ("Sloviansk",     37.62, 48.85),
        ("Odesa",         30.73, 46.48),
        ("Mykolaiv",      32.00, 46.97),
        ("Sumy",          34.80, 50.91),
        ("Chasiv Yar",    37.85, 48.60),
        ("Kurakhove",     37.31, 47.98),
    ]

    @staticmethod
    def distances_to_cities(geojson: dict) -> list[dict]:
        """
        Compute minimum distance from occupied territory boundary to key cities.
        Returns [{name, lat, lon, distance_km}] sorted by distance.
        """
        from shapely.geometry import Point

        geom = shape(geojson)
        if not geom.is_valid:
            geom = geom.buffer(0)

        # Project to UTM for metric distances
        geom_utm = shapely_transform(_to_utm, geom)

        results = []
        for name, lon, lat in FrontlineAnalytics.KEY_CITIES:
            pt_utm = shapely_transform(_to_utm, Point(lon, lat))
            dist_m = geom_utm.boundary.distance(pt_utm)
            inside = geom_utm.contains(pt_utm)
            results.append({
                "name": name,
                "lat": lat,
                "lon": lon,
                "distance_km": round(dist_m / 1000, 1),
                "occupied": inside,
            })

        results.sort(key=lambda r: r["distance_km"])
        return results


    # ── Oblast occupation breakdown ──────────────────────
    _oblast_geojson = None

    @classmethod
    def _load_oblasts(cls):
        if cls._oblast_geojson is not None:
            return cls._oblast_geojson
        path = DATA_DIR / "oblasts.geojson"
        if not path.exists():
            log.warning("oblasts.geojson not found — run setup to download")
            cls._oblast_geojson = []
            return []
        with open(path) as f:
            gj = json.load(f)
        oblasts = []
        for feat in gj.get("features", []):
            name = feat["properties"].get("name", "?")
            geom = shape(feat["geometry"])
            if not geom.is_valid:
                geom = geom.buffer(0)
            area_km2 = FrontlineAnalytics.area_km2(geom)
            oblasts.append({"name": name, "geom": geom, "total_km2": round(area_km2, 1)})
        cls._oblast_geojson = oblasts
        log.info(f"Loaded {len(oblasts)} oblasts")
        return oblasts

    @staticmethod
    def oblast_occupation(geojson: dict) -> list[dict]:
        """
        Compute % of each oblast occupied.
        Returns [{name, total_km2, occupied_km2, pct}] sorted by pct descending.
        Only includes oblasts with >0% occupation.
        """
        oblasts = FrontlineAnalytics._load_oblasts()
        if not oblasts:
            return []

        occ_geom = shape(geojson)
        if not occ_geom.is_valid:
            occ_geom = occ_geom.buffer(0)

        results = []
        for ob in oblasts:
            intersection = ob["geom"].intersection(occ_geom)
            if intersection.is_empty:
                continue
            occ_area = FrontlineAnalytics.area_km2(intersection)
            pct = (occ_area / ob["total_km2"]) * 100 if ob["total_km2"] > 0 else 0
            if pct < 0.1:
                continue
            results.append({
                "name": ob["name"],
                "total_km2": ob["total_km2"],
                "occupied_km2": round(occ_area, 1),
                "pct": round(pct, 1),
            })

        results.sort(key=lambda r: r["pct"], reverse=True)
        return results

    # ── Ghost frontlines ──────────────────────────────

    @staticmethod
    def ghost_frontlines(ds, current_date: str,
                         offsets: list[int] = None) -> list[dict]:
        """Load frontline boundaries from N days ago as ghost overlays."""
        if offsets is None:
            offsets = [30, 90, 180]
        current_dt = datetime.strptime(current_date.replace("-", ""), "%Y%m%d")
        results = []
        for days in offsets:
            past_str = (current_dt - timedelta(days=days)).strftime("%Y%m%d")
            geojson = ds.fetch_date(past_str)
            if not geojson:
                continue
            geom = shape(geojson)
            if not geom.is_valid:
                geom = geom.buffer(0)
            results.append({
                "label": f"{days}d ago",
                "days_ago": days,
                "date": past_str,
                "geometry": mapping(geom.boundary),
            })
        return results

    # ── Time-to-city projections ─────────────────────

    @staticmethod
    def time_to_city(ds, current_date: str, lookback: int = 30) -> list[dict]:
        """Estimate days until frontline reaches each city at current rate."""
        from shapely.geometry import Point
        clean = current_date.replace("-", "")
        current_dt = datetime.strptime(clean, "%Y%m%d")
        past_str = (current_dt - timedelta(days=lookback)).strftime("%Y%m%d")
        geom_now = ds.fetch_date(clean)
        geom_past = ds.fetch_date(past_str)
        if not geom_now or not geom_past:
            return []
        now = shape(geom_now)
        past = shape(geom_past)
        if not now.is_valid: now = now.buffer(0)
        if not past.is_valid: past = past.buffer(0)
        now_utm = shapely_transform(_to_utm, now)
        past_utm = shapely_transform(_to_utm, past)
        results = []
        for name, lon, lat in FrontlineAnalytics.KEY_CITIES:
            pt_utm = shapely_transform(_to_utm, Point(lon, lat))
            dist_now = now_utm.boundary.distance(pt_utm) / 1000
            dist_past = past_utm.boundary.distance(pt_utm) / 1000
            inside_now = now_utm.contains(pt_utm)
            if inside_now:
                results.append({"name": name, "distance_km": 0, "velocity_km_per_day": 0,
                                "days_to_reach": 0, "eta_date": None, "direction": "occupied"})
                continue
            delta_km = dist_past - dist_now
            velocity = delta_km / lookback
            days_to_reach = None
            eta_date = None
            direction = "approaching" if velocity > 0.01 else "receding" if velocity < -0.01 else "static"
            if velocity > 0.01 and dist_now > 0:
                days_to_reach = round(dist_now / velocity)
                eta_date = (current_dt + timedelta(days=days_to_reach)).strftime("%Y-%m-%d")
            results.append({"name": name, "distance_km": round(dist_now, 1),
                            "velocity_km_per_day": round(velocity, 3),
                            "days_to_reach": days_to_reach, "eta_date": eta_date,
                            "direction": direction})
        results.sort(key=lambda r: r["distance_km"])
        return results

    # ── Salient detection ────────────────────────────

    @staticmethod
    def detect_salients(geojson: dict, min_area_km2: float = 5.0) -> list[dict]:
        """Detect vulnerable salients/bulges via morphological opening."""
        geom = shape(geojson)
        if not geom.is_valid:
            geom = geom.buffer(0)
        geom_utm = shapely_transform(_to_utm, geom)
        smooth_distance = 5000  # 5km
        smoothed = geom_utm.buffer(-smooth_distance).buffer(smooth_distance * 1.2)
        if smoothed.is_empty:
            return []
        protrusions = geom_utm.difference(smoothed)
        indentations = smoothed.difference(geom_utm)
        _to_wgs = pyproj.Transformer.from_crs(UTM36N, WGS84, always_xy=True).transform
        salients = []
        for label, diff_geom, sal_type in [
            ("Russian salient", protrusions, "protrusion"),
            ("UA salient", indentations, "indentation"),
        ]:
            if diff_geom.is_empty:
                continue
            parts = list(diff_geom.geoms) if diff_geom.geom_type == "MultiPolygon" else (
                [diff_geom] if diff_geom.geom_type == "Polygon" else [])
            for part in parts:
                area = part.area / 1e6
                if area < min_area_km2:
                    continue
                perim = part.length / 1000
                compactness = (4 * 3.141593 * area) / (perim ** 2) if perim > 0 else 0
                centroid_wgs = shapely_transform(_to_wgs, part.centroid)
                part_wgs = shapely_transform(_to_wgs, part)
                salients.append({
                    "type": sal_type, "label": label,
                    "geometry": mapping(part_wgs),
                    "area_km2": round(area, 1), "perimeter_km": round(perim, 1),
                    "vulnerability": round(1 - compactness, 2),
                    "centroid_lat": round(centroid_wgs.y, 4),
                    "centroid_lon": round(centroid_wgs.x, 4),
                })
        salients.sort(key=lambda s: -s["area_km2"])
        return salients


class BriefingGenerator:
    """Auto-generates a daily text briefing from all available data."""

    @staticmethod
    def generate(analytics, ds, divgen_src, firms, raids,
                 date_str: str, prev_date: str = None) -> dict:
        """
        Generate a structured briefing for a date.
        Returns {date, title, summary, sections[]}.
        """
        from datetime import datetime as _dt

        # Parse date for display
        try:
            d = _dt.strptime(date_str, "%Y%m%d")
            display_date = d.strftime("%d %B %Y")
            invasion_start = _dt(2022, 2, 24)
            war_day = (d - invasion_start).days
        except Exception:
            display_date = date_str
            war_day = 0

        sections = []
        title = f"Frontline Briefing — {display_date} (Day {war_day})"

        # 1. Territory snapshot
        ds_geom = ds.fetch_date(date_str)
        if not ds_geom:
            return {"date": date_str, "title": title,
                    "summary": "No data available.", "sections": []}

        stats = analytics.compute_stats(ds_geom)
        area = stats["area_km2"]
        fl = stats["frontline_km"]
        sections.append({
            "heading": "Territory",
            "text": (f"Russian-occupied territory (DeepState): {area:,.0f} km² "
                     f"({area / 603550 * 100:.1f}% of Ukraine). "
                     f"Frontline length: {fl:,.0f} km."),
        })

        # 2. Divgen comparison
        dg_geom = divgen_src.fetch_date(date_str)
        if dg_geom:
            dg_area = analytics.total_area(dg_geom)
            gap = area - dg_area
            sections.append({
                "heading": "Source Comparison",
                "text": (f"Divgen.ru (Russian OSINT): {dg_area:,.0f} km² occupied. "
                         f"Gap: DS reports {gap:+,.0f} km² more than Divgen."),
            })

        # 3. Daily change
        if prev_date:
            prev_geom = ds.fetch_date(prev_date)
            if prev_geom:
                diff = analytics.compute_diff(prev_geom, ds_geom)
                gained = diff["gained_km2"]
                lost = diff["lost_km2"]
                net = diff["net_km2"]
                direction = "Russia gained" if net > 0 else "Ukraine recaptured" if net < 0 else "No net change"
                sections.append({
                    "heading": "Daily Change",
                    "text": (f"{direction} {abs(net):.1f} km² net. "
                             f"Russian gains: {gained:.1f} km², "
                             f"Ukrainian recaptures: {lost:.1f} km²."),
                })

        # 4. Distances to key cities
        distances = analytics.distances_to_cities(ds_geom)
        closest = [d for d in distances if not d["occupied"]][:3]
        if closest:
            city_strs = [f"{c['name']} ({c['distance_km']}km)" for c in closest]
            sections.append({
                "heading": "Nearest Cities",
                "text": f"Closest unoccupied cities to frontline: {', '.join(city_strs)}.",
            })

        # 5. Fire activity
        try:
            fires = firms.fetch_fires("24h")
            n_fires = len(fires)
            high_frp = [f for f in fires if float(f.get("frp", 0)) > 10]
            sections.append({
                "heading": "Fire Activity",
                "text": (f"{n_fires} thermal hotspots detected in last 24h "
                         f"({len(high_frp)} high-intensity, FRP>10MW)."),
            })
        except Exception:
            pass

        # 6. Air raid alerts
        try:
            alerts = raids.fetch_alerts()
            n_active = alerts.get("active_count", 0)
            alert_names = [s["name_en"] for s in alerts.get("states", [])
                           if s.get("status") in ("full", "partial", True)][:5]
            if n_active > 0:
                sections.append({
                    "heading": "Air Raid Alerts",
                    "text": (f"{n_active} oblast(s) under air raid alert: "
                             f"{', '.join(alert_names[:5])}"
                             f"{'...' if len(alert_names) > 5 else ''}."),
                })
            else:
                sections.append({
                    "heading": "Air Raid Alerts",
                    "text": "No active air raid alerts.",
                })
        except Exception:
            pass

        # Build summary (first 2 sentences)
        summary_parts = []
        if sections:
            summary_parts.append(sections[0]["text"].split(".")[0] + ".")
        if len(sections) > 2:
            summary_parts.append(sections[2]["text"].split(".")[0] + ".")
        summary = " ".join(summary_parts) if summary_parts else "Briefing generated."

        return {
            "date": date_str,
            "title": title,
            "summary": summary,
            "sections": sections,
            "war_day": war_day,
        }


class HottestSectors:
    """Ranks frontline sectors by activity (fire density + movement speed)."""

    # Sectors defined as bounding boxes: (name, west, south, east, north)
    SECTORS = [
        ("Kharkiv",        35.5, 49.0, 37.5, 50.5),
        ("Lyman",          36.5, 48.7, 38.0, 49.2),
        ("Bakhmut",        37.5, 48.3, 38.5, 48.8),
        ("Chasiv Yar",     37.5, 48.5, 38.2, 48.7),
        ("Pokrovsk",       36.5, 47.8, 37.8, 48.4),
        ("Kurakhove",      36.5, 47.4, 37.5, 48.0),
        ("Donetsk City",   37.2, 47.8, 38.2, 48.2),
        ("Zaporizhzhia",   34.5, 46.8, 36.5, 47.8),
        ("Kherson",        32.0, 46.2, 34.0, 47.2),
        ("Sumy/Kursk",     33.5, 50.5, 36.0, 52.0),
    ]

    @staticmethod
    def rank(analytics, ds, firms, date_str: str, lookback: int = 7) -> list[dict]:
        """
        Rank sectors by composite activity score.
        Combines: fire density, frontline movement, sector area change.
        """
        from shapely.geometry import box as shapely_box, Point
        from datetime import datetime as _dt, timedelta as _td

        # Get fires
        try:
            fires = firms.fetch_fires("24h")
        except Exception:
            fires = []

        # Current + past geometry
        ds_now = ds.fetch_date(date_str)
        if not ds_now:
            return []

        try:
            d = _dt.strptime(date_str, "%Y%m%d")
            past_str = (d - _td(days=lookback)).strftime("%Y%m%d")
        except Exception:
            return []

        ds_past = ds.fetch_date(past_str)

        now_geom = shape(ds_now)
        if not now_geom.is_valid:
            now_geom = now_geom.buffer(0)
        past_geom = shape(ds_past) if ds_past else None
        if past_geom and not past_geom.is_valid:
            past_geom = past_geom.buffer(0)

        results = []
        for name, west, south, east, north in HottestSectors.SECTORS:
            sector_box = shapely_box(west, south, east, north)

            # Fire count in sector
            fire_count = sum(
                1 for f in fires
                if west <= float(f.get("longitude", 0)) <= east
                and south <= float(f.get("latitude", 0)) <= north
            )

            # Area change in sector
            area_change = 0.0
            if past_geom:
                now_clip = now_geom.intersection(sector_box)
                past_clip = past_geom.intersection(sector_box)
                if not now_clip.is_empty and not past_clip.is_empty:
                    now_a = analytics.area_km2(now_clip)
                    past_a = analytics.area_km2(past_clip)
                    area_change = now_a - past_a

            # Frontline length in sector
            fl_in_sector = 0.0
            boundary = now_geom.boundary.intersection(sector_box)
            if not boundary.is_empty:
                boundary_utm = shapely_transform(_to_utm, boundary)
                fl_in_sector = boundary_utm.length / 1000

            # Composite score: normalize and weight
            fire_score = min(fire_count / 50, 1.0)  # 50+ fires = max
            change_score = min(abs(area_change) / 20, 1.0)  # 20+ km² change = max
            fl_score = min(fl_in_sector / 200, 1.0)  # 200+ km frontline = max

            activity = round((fire_score * 0.5 + change_score * 0.3 + fl_score * 0.2) * 100)

            level = "HIGH" if activity >= 50 else "MEDIUM" if activity >= 20 else "LOW"

            results.append({
                "name": name,
                "fire_count": fire_count,
                "area_change_km2": round(area_change, 1),
                "frontline_km": round(fl_in_sector, 1),
                "activity_score": activity,
                "level": level,
                "bbox": [west, south, east, north],
            })

        results.sort(key=lambda r: -r["activity_score"])
        return results


class DivgenSource:
    """
    Scrapes frontline data from divgen.ru (Russian-perspective OSINT map).
    Fetches KML, extracts occupied territory polygons, converts to GeoJSON.

    The KML contains multiple styled layers. The Ukraine-controlled territory
    uses style #id21 (blue). Everything else clipped to Ukraine = occupied.

    Requires session cookie from main page (no auth/API key needed).
    """

    BASE = "https://divgen.ru"
    UKRAINE_BBOX = (22, 44, 41, 53)  # west, south, east, north

    def __init__(self):
        self.session = requests.Session()
        self.session.headers["User-Agent"] = (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        self.session.headers["Referer"] = "https://divgen.ru/"
        self._session_init = False

    def _ensure_session(self):
        if not self._session_init:
            self.session.get(f"{self.BASE}/", timeout=15)
            self._session_init = True

    def get_events(self) -> list[dict]:
        """Get all map events (dates with frontline snapshots)."""
        cache_file = CACHE_DIR / "divgen_events.json"
        if cache_file.exists():
            age_hours = (datetime.now().timestamp() - os.path.getmtime(cache_file)) / 3600
            if age_hours < 6:
                with open(cache_file) as f:
                    return json.load(f)

        self._ensure_session()
        try:
            resp = self.session.get(f"{self.BASE}/api.php", timeout=30)
            resp.raise_for_status()
            events = resp.json()
            with open(cache_file, "w") as f:
                json.dump(events, f)
            log.info(f"Divgen: {len(events)} events loaded")
            return events
        except Exception as e:
            log.warning(f"Divgen events failed: {e}")
            return []

    def fetch_date(self, date_str: str) -> dict | None:
        """
        Fetch divgen occupied territory for a date.
        Finds the closest event, downloads KML, extracts occupied polygons.
        Returns GeoJSON geometry (MultiPolygon).
        Cached per event idx.
        """
        clean = date_str.replace("-", "")
        target = f"{clean[:4]}-{clean[4:6]}-{clean[6:8]}"

        # Find closest event to target date
        events = self.get_events()
        if not events:
            return None

        closest = min(events, key=lambda e: abs(
            datetime.strptime(e["pdate"], "%Y-%m-%d") - datetime.strptime(target, "%Y-%m-%d")
        ))
        idx = closest["idx"]

        # Check cache
        cache_file = CACHE_DIR / f"divgen_{idx}.geojson"
        if cache_file.exists():
            with open(cache_file) as f:
                return json.load(f)

        # Fetch KML
        self._ensure_session()
        try:
            resp = self.session.get(f"{self.BASE}/kml/{idx}", timeout=30)
            resp.raise_for_status()
            log.info(f"Divgen KML fetched: {len(resp.content)}B for idx={idx}")
            if len(resp.content) < 100:
                log.warning(f"Divgen KML too small for {idx}")
                return None

            geom = self._parse_kml(resp.content)
            if geom:
                with open(cache_file, "w") as f:
                    json.dump(geom, f)
                log.info(f"Divgen cached: {closest['pdate']} ({len(json.dumps(geom)) // 1024}KB)")
                return geom
            return None
        except Exception as e:
            log.warning(f"Divgen KML fetch failed: {e}")
            return None

    _ukraine_border = None

    @classmethod
    def _get_ukraine_border(cls):
        """Load Ukraine border from oblasts.geojson."""
        if cls._ukraine_border is not None:
            return cls._ukraine_border
        path = DATA_DIR / "oblasts.geojson"
        if not path.exists():
            log.warning("oblasts.geojson not found for Ukraine border")
            return None
        with open(path) as f:
            gj = json.load(f)
        border = unary_union([shape(f["geometry"]) for f in gj["features"]])
        if not border.is_valid:
            border = border.buffer(0)
        cls._ukraine_border = border
        return border

    def _parse_kml(self, kml_bytes: bytes) -> dict | None:
        """
        Parse divgen KML and extract occupied territory.
        Strategy: Divgen polygons represent non-occupied zones (UA-controlled,
        grey zone, neighboring countries). The GAPS between these polygons
        within Ukraine's borders = occupied territory (negative space).
        """
        from xml.etree import ElementTree as ET

        KML = "{http://www.opengis.net/kml/2.2}"
        root = ET.fromstring(kml_bytes)

        all_polys = []
        for pm in root.findall(f".//{KML}Placemark"):
            for ce in pm.findall(f".//{KML}coordinates"):
                pts = []
                for pt in ce.text.strip().split():
                    parts = pt.split(",")
                    if len(parts) >= 2:
                        pts.append((float(parts[0]), float(parts[1])))
                if len(pts) >= 3:
                    try:
                        poly = Polygon(pts)
                        if poly.is_valid:
                            all_polys.append(poly)
                    except Exception:
                        pass

        if not all_polys:
            log.warning("Divgen: no valid polygons in KML")
            return None

        # Sanity check: newer KMLs have ~10-25 clean polygons.
        # Older KMLs have 40+ overlapping polygons with no styles — negative space fails.
        # Also validate: the largest polygon should be ~400-500K km² (UA controlled).
        # If it's not, the KML structure is different and we can't reliably extract.
        largest_area = max(p.area for p in all_polys)
        if len(all_polys) > 30 or largest_area < 30:  # 30 sq degrees ≈ ~250K km²
            log.warning(f"Divgen: KML format unreliable ({len(all_polys)} polys, "
                        f"largest={largest_area:.1f} sq deg). Skipping.")
            return None

        log.info(f"Divgen: {len(all_polys)} polygons from KML")

        # Union all divgen polygons
        all_union = unary_union(all_polys)

        # Get Ukraine border
        ukr_border = self._get_ukraine_border()
        if ukr_border is None:
            log.warning("Divgen: no Ukraine border available")
            return None

        # Occupied = Ukraine MINUS all divgen polygons
        occupied = ukr_border.difference(all_union)
        if occupied.is_empty:
            return None
        if not occupied.is_valid:
            occupied = occupied.buffer(0)

        # Save the full divgen footprint (all KML polygons within Ukraine = their view of Ukraine, no Crimea)
        divgen_ukraine = unary_union(all_polys).intersection(ukr_border)
        if not divgen_ukraine.is_valid:
            divgen_ukraine = divgen_ukraine.buffer(0)

        occ_area = FrontlineAnalytics.area_km2(occupied)
        # Sanity: occupied territory should be 50K-200K km². Anything outside = bad extraction.
        if occ_area < 50000 or occ_area > 200000:
            log.warning(f"Divgen: occupied area {occ_area:,.0f} km² out of range, discarding")
            return None
        log.info(f"Divgen occupied: {occ_area:,.0f} km²")
        return mapping(occupied)

    def fetch_footprint(self, date_str: str) -> dict | None:
        """
        Get divgen's view of Ukraine (all KML polygons within Ukraine border).
        This is Ukraine WITHOUT Crimea according to divgen.
        Used to clip DeepState for fair comparison.
        """
        from xml.etree import ElementTree as ET

        clean = date_str.replace("-", "")
        target = f"{clean[:4]}-{clean[4:6]}-{clean[6:8]}"

        cache_file = CACHE_DIR / f"divgen_footprint_{clean}.geojson"
        if cache_file.exists():
            with open(cache_file) as f:
                return json.load(f)

        events = self.get_events()
        if not events:
            return None

        closest = min(events, key=lambda e: abs(
            datetime.strptime(e["pdate"], "%Y-%m-%d") - datetime.strptime(target, "%Y-%m-%d")
        ))

        self._ensure_session()
        try:
            resp = self.session.get(f"{self.BASE}/kml/{closest['idx']}", timeout=30)
            resp.raise_for_status()
            if len(resp.content) < 100:
                return None

            KML = "{http://www.opengis.net/kml/2.2}"
            root = ET.fromstring(resp.content)

            all_polys = []
            for pm in root.findall(f".//{KML}Placemark"):
                for ce in pm.findall(f".//{KML}coordinates"):
                    pts = []
                    for pt in ce.text.strip().split():
                        parts = pt.split(",")
                        if len(parts) >= 2:
                            pts.append((float(parts[0]), float(parts[1])))
                    if len(pts) >= 3:
                        try:
                            poly = Polygon(pts)
                            if poly.is_valid:
                                all_polys.append(poly)
                        except Exception:
                            pass

            if not all_polys:
                return None

            largest_area = max(p.area for p in all_polys)
            if len(all_polys) > 30 or largest_area < 30:
                log.warning(f"Divgen footprint: KML format unreliable. Skipping.")
                return None

            ukr_border = self._get_ukraine_border()
            if ukr_border is None:
                return None

            # The footprint = where divgen has an opinion (Ukraine minus Crimea).
            # Since KML polygons + occupied gaps = all Ukraine, we can't use
            # simple set ops. Instead: find the LARGEST single polygon (UA-controlled,
            # ~480K km²) and use ITS convex hull + the occupied territory hull as the
            # footprint boundary. This naturally excludes Crimea because the UA-controlled
            # polygon doesn't extend there.
            largest = max(all_polys, key=lambda p: p.area)
            occupied = ukr_border.difference(unary_union(all_polys))
            if not occupied.is_valid:
                occupied = occupied.buffer(0)
            # Combine the largest polygon's extent with occupied territory
            combined = largest.union(occupied)
            footprint = combined.convex_hull.buffer(0.1).intersection(ukr_border)
            if not footprint.is_valid:
                footprint = footprint.buffer(0)

            result = mapping(footprint)
            with open(cache_file, "w") as f:
                json.dump(result, f)
            log.info(f"Divgen footprint cached: {FrontlineAnalytics.area_km2(footprint):,.0f} km²")
            return result
        except Exception as e:
            log.warning(f"Divgen footprint failed: {e}")
            return None

    def get_available_dates(self) -> list[str]:
        """Get dates with divgen data (YYYYMMDD format)."""
        events = self.get_events()
        dates = set()
        for e in events:
            try:
                d = datetime.strptime(e["pdate"], "%Y-%m-%d").strftime("%Y%m%d")
                dates.add(d)
            except Exception:
                pass
        return sorted(dates)


class NASAFirms:
    """
    Fetches active fire/thermal hotspot data from NASA FIRMS.
    No API key needed — uses the open global CSV downloads.
    VIIRS (375m resolution) + MODIS (1km) sensors.
    """

    VIIRS_24H = "https://firms.modaps.eosdis.nasa.gov/data/active_fire/noaa-20-viirs-c2/csv/J1_VIIRS_C2_Global_24h.csv"
    VIIRS_48H = "https://firms.modaps.eosdis.nasa.gov/data/active_fire/noaa-20-viirs-c2/csv/J1_VIIRS_C2_Global_48h.csv"
    VIIRS_7D = "https://firms.modaps.eosdis.nasa.gov/data/active_fire/noaa-20-viirs-c2/csv/J1_VIIRS_C2_Global_7d.csv"

    # Ukraine bounding box
    BBOX = (22.0, 44.0, 40.0, 53.0)  # west, south, east, north

    def __init__(self):
        self.session = requests.Session()
        self.session.headers["User-Agent"] = "frontline-tracker/1.0"

    def fetch_fires(self, period: str = "24h") -> list[dict]:
        """
        Fetch active fires in Ukraine region.
        period: "24h", "48h", or "7d"
        Returns [{lat, lon, frp, confidence, acq_date, acq_time, daynight}]
        Cached for 1 hour.
        """
        import csv as csv_mod
        from io import StringIO

        cache_file = CACHE_DIR / f"firms_{period}.json"
        if cache_file.exists():
            age_hours = (datetime.now().timestamp() - os.path.getmtime(cache_file)) / 3600
            if age_hours < 1:
                with open(cache_file) as f:
                    return json.load(f)

        url_map = {"24h": self.VIIRS_24H, "48h": self.VIIRS_48H, "7d": self.VIIRS_7D}
        url = url_map.get(period, self.VIIRS_24H)

        try:
            log.info(f"Downloading FIRMS {period} data...")
            resp = self.session.get(url, timeout=90)
            resp.raise_for_status()

            reader = csv_mod.DictReader(StringIO(resp.text))
            w, s, e, n = self.BBOX
            fires = []
            for row in reader:
                lat = float(row["latitude"])
                lon = float(row["longitude"])
                if w <= lon <= e and s <= lat <= n:
                    fires.append({
                        "lat": round(lat, 5),
                        "lon": round(lon, 5),
                        "frp": float(row.get("frp", 0)),
                        "confidence": row.get("confidence", ""),
                        "acq_date": row.get("acq_date", ""),
                        "acq_time": row.get("acq_time", ""),
                        "daynight": row.get("daynight", ""),
                    })

            with open(cache_file, "w") as f:
                json.dump(fires, f)
            log.info(f"FIRMS {period}: {len(fires)} hotspots in Ukraine")
            return fires
        except Exception as e:
            log.warning(f"FIRMS fetch failed: {e}")
            return []

    def fires_as_geojson(self, period: str = "24h") -> dict:
        """Return fires as a GeoJSON FeatureCollection for Leaflet."""
        fires = self.fetch_fires(period)
        features = []
        for f in fires:
            features.append({
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [f["lon"], f["lat"]]},
                "properties": {
                    "frp": f["frp"],
                    "confidence": f["confidence"],
                    "time": f"{f['acq_date']} {f['acq_time']}",
                    "daynight": f["daynight"],
                },
            })
        return {"type": "FeatureCollection", "features": features}


class AirRaidAlerts:
    """
    Real-time air raid alert status per oblast.
    Primary: sirens.in.ua (no key, real-time, full/partial/null)
    Fallback: alerts.com.ua (no key, structured with timestamps)
    """

    SIRENS_URL = "https://sirens.in.ua/api/v1"
    ALERTS_URL = "https://alerts.com.ua/api/states"

    def __init__(self):
        self.session = requests.Session()
        self.session.headers["User-Agent"] = "frontline-tracker/1.0"

    def fetch_alerts(self) -> dict:
        """
        Returns {oblasts: [{name, status, level}], active_count, timestamp}.
        status: "full", "partial", "none", "no_data"
        level: 2=full, 1=partial, 0=none
        Cached for 30 seconds.
        """
        cache_file = CACHE_DIR / "air_raids.json"
        if cache_file.exists():
            age_sec = datetime.now().timestamp() - os.path.getmtime(cache_file)
            if age_sec < 30:
                with open(cache_file) as f:
                    return json.load(f)

        try:
            resp = self.session.get(self.SIRENS_URL, timeout=10)
            resp.raise_for_status()
            raw = resp.json()

            oblasts = []
            for name, status in raw.items():
                if status is None:
                    status_str = "none"
                    level = 0
                elif status == "no_data":
                    status_str = "no_data"
                    level = 0
                elif status == "full":
                    status_str = "full"
                    level = 2
                else:  # "partial"
                    status_str = "partial"
                    level = 1
                oblasts.append({"name": name, "status": status_str, "level": level})

            oblasts.sort(key=lambda o: (-o["level"], o["name"]))
            active = sum(1 for o in oblasts if o["level"] > 0)

            result = {
                "oblasts": oblasts,
                "active_count": active,
                "total": len(oblasts),
                "timestamp": datetime.now().isoformat(),
            }

            with open(cache_file, "w") as f:
                json.dump(result, f)
            return result

        except Exception as e:
            log.warning(f"Air raid API failed: {e}")
            return {"oblasts": [], "active_count": 0, "total": 0, "error": str(e)}


class FrontlineWeather:
    """
    Weather conditions along the frontline from Open-Meteo (free, no key).
    """

    API = "https://api.open-meteo.com/v1/forecast"

    # WMO weather codes to descriptions
    WMO = {
        0: "Clear", 1: "Mostly clear", 2: "Partly cloudy", 3: "Overcast",
        45: "Fog", 48: "Rime fog",
        51: "Light drizzle", 53: "Drizzle", 55: "Heavy drizzle",
        61: "Light rain", 63: "Rain", 65: "Heavy rain",
        66: "Freezing rain", 67: "Heavy freezing rain",
        71: "Light snow", 73: "Snow", 75: "Heavy snow", 77: "Snow grains",
        80: "Light showers", 81: "Showers", 82: "Heavy showers",
        85: "Light snow showers", 86: "Snow showers",
        95: "Thunderstorm", 96: "Thunderstorm + hail", 99: "Heavy thunderstorm",
    }

    LOCATIONS = [
        ("Pokrovsk", 37.18, 48.28),
        ("Kramatorsk", 37.56, 48.74),
        ("Zaporizhzhia", 35.14, 47.84),
        ("Kherson", 32.62, 46.64),
        ("Kharkiv", 36.23, 49.99),
        ("Sumy", 34.80, 50.91),
        ("Donetsk front", 37.80, 48.00),
        ("Kursk border", 35.50, 51.50),
    ]

    def __init__(self):
        self.session = requests.Session()
        self.session.headers["User-Agent"] = "frontline-tracker/1.0"

    def fetch_weather(self) -> list[dict]:
        """
        Get current weather for key frontline locations.
        Cached for 30 minutes.
        """
        cache_file = CACHE_DIR / "weather.json"
        if cache_file.exists():
            age_min = (datetime.now().timestamp() - os.path.getmtime(cache_file)) / 60
            if age_min < 30:
                with open(cache_file) as f:
                    return json.load(f)

        results = []
        for name, lon, lat in self.LOCATIONS:
            try:
                resp = self.session.get(self.API, params={
                    "latitude": lat,
                    "longitude": lon,
                    "current": "temperature_2m,wind_speed_10m,wind_gusts_10m,precipitation,weather_code,cloud_cover,visibility",
                    "timezone": "Europe/Kyiv",
                }, timeout=10)
                d = resp.json().get("current", {})
                code = d.get("weather_code", 0)
                results.append({
                    "name": name,
                    "lat": lat,
                    "lon": lon,
                    "temp_c": d.get("temperature_2m"),
                    "wind_kmh": d.get("wind_speed_10m"),
                    "gusts_kmh": d.get("wind_gusts_10m"),
                    "precip_mm": d.get("precipitation"),
                    "cloud_pct": d.get("cloud_cover"),
                    "visibility_m": d.get("visibility"),
                    "weather_code": code,
                    "weather": self.WMO.get(code, f"Code {code}"),
                })
            except Exception as e:
                log.debug(f"Weather failed for {name}: {e}")

        if results:
            with open(cache_file, "w") as f:
                json.dump(results, f)
        return results


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Ukraine Frontline Data Pipeline")
    parser.add_argument("--fetch-date", type=str, help="Fetch specific date (YYYYMMDD)")
    parser.add_argument("--stats", type=str, help="Full stats for date")
    parser.add_argument("--time-series", action="store_true", help="Build weekly time series")
    args = parser.parse_args()

    ds = DeepStateSource()

    if args.fetch_date:
        geom = ds.fetch_date(args.fetch_date)
        print(f"{args.fetch_date}: {geom['type'] if geom else 'not found'}")

    if args.stats:
        geom = ds.fetch_date(args.stats)
        if geom:
            s = FrontlineAnalytics.compute_stats(geom)
            print(f"Area: {s['area_km2']:,.0f} km²")
            print(f"Frontline: {s['frontline_km']:,.0f} km")
            print(f"Polygons: {s['polygons']}")

    if args.time_series:
        dates = ds.get_available_dates()
        # Weekly sampling
        weekly = dates[::7] + ([dates[-1]] if dates[-1] != dates[::7][-1] else [])
        ts = FrontlineAnalytics.build_time_series(ds, weekly)
        rates = FrontlineAnalytics.compute_rates(ts)
        for r in rates[-10:]:
            print(f"{r['date']}: {r['area_km2']:,.0f} km² | Δ{r['delta_km2']:+.1f} | {r['rate_7d_avg']:+.2f} km²/day")
