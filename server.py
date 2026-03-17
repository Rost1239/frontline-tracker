"""
Ukraine Frontline Tracker — Web Server
=======================================
Flask app with DeepState frontline data + analytical endpoints.

Run:  python server.py
Open: http://localhost:5001
"""

import csv
import io
import json
import logging
import threading
from pathlib import Path

log = logging.getLogger(__name__)

from flask import Flask, render_template, jsonify, request, Response
from data_pipeline import (
    DeepStateSource, FrontlineAnalytics, NASAFirms,
    DivgenSource, AirRaidAlerts, FrontlineWeather, DATA_DIR,
)

app = Flask(__name__, static_folder="static", template_folder="templates")

ds = DeepStateSource()
analytics = FrontlineAnalytics()
firms = NASAFirms()
divgen = DivgenSource()
raids = AirRaidAlerts()
weather = FrontlineWeather()

# Pre-cached time series (built in background on startup)
_ts_ds = []       # DeepState time series
_ts_divgen = []   # Divgen time series
_ts_lock = threading.Lock()


def _precache_time_series():
    """Build weekly time series for both sources in background."""
    global _ts_ds, _ts_divgen

    try:
        dates = ds.get_available_dates()
        if not dates:
            return

        weekly = dates[::7]
        if dates[-1] != weekly[-1]:
            weekly.append(dates[-1])

        # DeepState first (faster)
        ts = analytics.build_time_series(ds, weekly, "time_series")
        rates = analytics.compute_rates(ts)
        with _ts_lock:
            _ts_ds = rates
        log.info(f"DeepState time series ready: {len(rates)} points")

        # Divgen (slower — scrapes KML per date)
        log.info("Building Divgen time series (this takes a few minutes first time)...")
        ts_dg = analytics.build_time_series(divgen, weekly, "divgen_time_series")
        rates_dg = analytics.compute_rates(ts_dg)
        with _ts_lock:
            _ts_divgen = rates_dg
        log.info(f"Divgen time series ready: {len(rates_dg)} points")
    except Exception as e:
        log.error(f"Time series build failed: {e}", exc_info=True)


@app.route("/")
def index():
    return render_template("index.html")


# ── Core data ────────────────────────────────────────────

@app.route("/api/dates")
def api_dates():
    return jsonify(ds.get_available_dates())


@app.route("/api/date/<date_str>")
def api_date(date_str):
    geom = ds.fetch_date(date_str)
    if geom:
        return jsonify(geom)
    return jsonify({"error": f"No data for {date_str}"}), 404


# ── Snapshot (batch endpoint) ─────────────────────────────

@app.route("/api/snapshot/<date_str>")
def api_snapshot(date_str):
    """
    Single endpoint returning everything needed for a date change.
    Replaces 6+ parallel calls with one request.
    Returns: {ds_geom, dg_geom, stats, compare, distances, oblasts, diff}
    """
    from shapely.geometry import shape, mapping as geom_mapping

    result = {"date": date_str}

    # DeepState geometry + stats
    ds_geom = ds.fetch_date(date_str)
    if not ds_geom:
        return jsonify({"error": f"No data for {date_str}"}), 404

    result["ds_geom"] = ds_geom
    result["stats"] = analytics.compute_stats(ds_geom)

    # Divgen geometry
    dg_geom = divgen.fetch_date(date_str)
    result["dg_geom"] = dg_geom

    # Compare (DS vs Divgen, clipped)
    if dg_geom:
        ds_g = shape(ds_geom)
        dg_g = shape(dg_geom)
        if not ds_g.is_valid: ds_g = ds_g.buffer(0)
        if not dg_g.is_valid: dg_g = dg_g.buffer(0)

        footprint_geom = divgen.fetch_footprint(date_str)
        if footprint_geom:
            fp = shape(footprint_geom)
            if not fp.is_valid: fp = fp.buffer(0)
            ds_clipped = ds_g.intersection(fp)
        else:
            ds_clipped = ds_g
        if not ds_clipped.is_valid: ds_clipped = ds_clipped.buffer(0)

        excluded = ds_g.difference(ds_clipped)
        ds_only = ds_clipped.difference(dg_g)
        dg_only = dg_g.difference(ds_clipped)
        overlap = ds_clipped.intersection(dg_g)

        result["compare"] = {
            "ds_only": geom_mapping(ds_only) if not ds_only.is_empty else None,
            "dg_only": geom_mapping(dg_only) if not dg_only.is_empty else None,
            "ds_area_km2": round(analytics.area_km2(ds_clipped), 1),
            "ds_total_km2": round(analytics.area_km2(ds_g), 1),
            "dg_area_km2": round(analytics.area_km2(dg_g), 1),
            "ds_only_km2": round(analytics.area_km2(ds_only), 1),
            "dg_only_km2": round(analytics.area_km2(dg_only), 1),
            "overlap_km2": round(analytics.area_km2(overlap), 1),
            "excluded_km2": round(analytics.area_km2(excluded), 1),
        }

    # Distances
    result["distances"] = analytics.distances_to_cities(ds_geom)

    # Oblasts
    result["oblasts"] = analytics.oblast_occupation(ds_geom)

    return jsonify(result)


# ── Analytics ────────────────────────────────────────────

@app.route("/api/stats/<date_str>")
def api_stats(date_str):
    """Full stats: area_km2, frontline_km, polygons."""
    geom = ds.fetch_date(date_str)
    if geom:
        stats = analytics.compute_stats(geom)
        stats["date"] = date_str
        return jsonify(stats)
    return jsonify({"error": f"No data for {date_str}"}), 404


@app.route("/api/diff/<date1>/<date2>")
def api_diff(date1, date2):
    """Territorial diff: gained/lost geometry + areas."""
    geom1 = ds.fetch_date(date1)
    geom2 = ds.fetch_date(date2)
    if geom1 and geom2:
        return jsonify(analytics.compute_diff(geom1, geom2))
    missing = [d for d, g in [(date1, geom1), (date2, geom2)] if not g]
    return jsonify({"error": f"No data for: {', '.join(missing)}"}), 404


@app.route("/api/time-series")
def api_time_series():
    """
    Area + frontline time series with rate-of-advance.
    ?source=ds|divgen (default: ds)
    """
    source = request.args.get("source", "ds")
    with _ts_lock:
        data = list(_ts_divgen if source == "divgen" else _ts_ds)
    if not data:
        return jsonify({"error": f"Time series ({source}) not ready yet, try again in a moment"}), 503
    return jsonify(data)


@app.route("/api/time-series/dual")
def api_time_series_dual():
    """Both DS and Divgen time series for dual-line chart."""
    with _ts_lock:
        ds_data = list(_ts_ds)
        dg_data = list(_ts_divgen)
    if not ds_data:
        return jsonify({"error": "DeepState time series not ready"}), 503
    return jsonify({"deepstate": ds_data, "divgen": dg_data})


@app.route("/api/heatmap/<date_str>")
def api_heatmap(date_str):
    """
    Change heatmap: union of all gained/lost over last N days.
    ?days=30 (lookback period, default 30)
    """
    days = request.args.get("days", 30, type=int)
    days = min(max(days, 7), 90)  # clamp 7-90
    result = analytics.change_heatmap(ds, date_str, lookback=days)
    if result:
        return jsonify(result)
    return jsonify({"error": f"No data for heatmap around {date_str}"}), 404


# ── Distance to cities ───────────────────────────────────

@app.route("/api/distances/<date_str>")
def api_distances(date_str):
    """Distance from frontline to key cities in km."""
    geom = ds.fetch_date(date_str)
    if geom:
        return jsonify(analytics.distances_to_cities(geom))
    return jsonify({"error": f"No data for {date_str}"}), 404


# ── Oblast occupation ────────────────────────────────────

@app.route("/api/oblasts/<date_str>")
def api_oblasts(date_str):
    """Oblast-level occupation breakdown."""
    geom = ds.fetch_date(date_str)
    if geom:
        return jsonify(analytics.oblast_occupation(geom))
    return jsonify({"error": f"No data for {date_str}"}), 404


# ── Divgen (Russian perspective) ──────────────────────────

@app.route("/api/divgen/date/<date_str>")
def api_divgen_date(date_str):
    """Divgen.ru occupied territory for a date (Russian perspective)."""
    geom = divgen.fetch_date(date_str)
    if geom:
        return jsonify(geom)
    return jsonify({"error": f"No divgen data for {date_str}"}), 404


@app.route("/api/divgen/area/<date_str>")
def api_divgen_area(date_str):
    """Divgen occupied area in km²."""
    geom = divgen.fetch_date(date_str)
    if geom:
        area = analytics.total_area(geom)
        return jsonify({"date": date_str, "source": "divgen", "area_km2": area})
    return jsonify({"error": f"No divgen data for {date_str}"}), 404


@app.route("/api/divgen/diff/<date1>/<date2>")
def api_divgen_diff(date1, date2):
    """Divgen territorial diff between two dates."""
    geom1 = divgen.fetch_date(date1)
    geom2 = divgen.fetch_date(date2)
    if geom1 and geom2:
        return jsonify(analytics.compute_diff(geom1, geom2))
    return jsonify({"error": "Missing divgen data"}), 404


@app.route("/api/diff/dual/<date1>/<date2>")
def api_diff_dual(date1, date2):
    """Both DS and Divgen diffs in one call."""
    ds1, ds2 = ds.fetch_date(date1), ds.fetch_date(date2)
    dg1, dg2 = divgen.fetch_date(date1), divgen.fetch_date(date2)
    result = {"ds": None, "divgen": None}
    if ds1 and ds2:
        result["ds"] = analytics.compute_diff(ds1, ds2)
    if dg1 and dg2:
        result["divgen"] = analytics.compute_diff(dg1, dg2)
    return jsonify(result)


@app.route("/api/compare/<date_str>")
def api_compare(date_str):
    """
    Compare DeepState vs Divgen occupied territory.
    ?exclude_crimea=1 to exclude Crimea from DS for fair comparison.
    """
    from shapely.geometry import shape, mapping as geom_mapping

    ds_geom = ds.fetch_date(date_str)
    dg_geom = divgen.fetch_date(date_str)
    if not ds_geom:
        return jsonify({"error": f"No DeepState data for {date_str}"}), 404
    if not dg_geom:
        return jsonify({"error": f"No Divgen data for {date_str}"}), 404

    ds_g = shape(ds_geom)
    dg_g = shape(dg_geom)
    if not ds_g.is_valid: ds_g = ds_g.buffer(0)
    if not dg_g.is_valid: dg_g = dg_g.buffer(0)

    # Fair comparison: clip DS to divgen's footprint (their view of Ukraine, no Crimea).
    # This uses divgen's actual Crimea border, not a hardcoded bounding box.
    footprint_geom = divgen.fetch_footprint(date_str)
    if footprint_geom:
        fp = shape(footprint_geom)
        if not fp.is_valid: fp = fp.buffer(0)
        ds_clipped = ds_g.intersection(fp)
    else:
        ds_clipped = ds_g  # fallback: no clipping

    if not ds_clipped.is_valid: ds_clipped = ds_clipped.buffer(0)

    excluded = ds_g.difference(ds_clipped)
    excluded_km2 = round(analytics.area_km2(excluded), 1)

    ds_only = ds_clipped.difference(dg_g)
    dg_only = dg_g.difference(ds_clipped)
    overlap = ds_clipped.intersection(dg_g)

    return jsonify({
        "ds_only": geom_mapping(ds_only) if not ds_only.is_empty else None,
        "dg_only": geom_mapping(dg_only) if not dg_only.is_empty else None,
        "overlap": geom_mapping(overlap) if not overlap.is_empty else None,
        "ds_area_km2": round(analytics.area_km2(ds_clipped), 1),
        "ds_total_km2": round(analytics.area_km2(ds_g), 1),
        "dg_area_km2": round(analytics.area_km2(dg_g), 1),
        "ds_only_km2": round(analytics.area_km2(ds_only), 1),
        "dg_only_km2": round(analytics.area_km2(dg_only), 1),
        "overlap_km2": round(analytics.area_km2(overlap), 1),
        "excluded_km2": excluded_km2,
    })


# ── NASA FIRMS fire data ─────────────────────────────────

@app.route("/api/fires")
def api_fires():
    """
    Active fire/thermal hotspots in Ukraine from NASA FIRMS (VIIRS satellite).
    ?period=24h|48h|7d (default: 24h)
    Returns GeoJSON FeatureCollection with FRP (fire radiative power) per point.
    """
    period = request.args.get("period", "24h")
    if period not in ("24h", "48h", "7d"):
        period = "24h"
    return jsonify(firms.fires_as_geojson(period))


# ── Ghost frontlines ─────────────────────────────────────

@app.route("/api/ghosts/<date_str>")
def api_ghosts(date_str):
    """Frontline boundaries from 30/90/180 days ago."""
    offsets = request.args.get("offsets", "30,90,180")
    offsets = [int(x) for x in offsets.split(",") if x.isdigit()][:5]  # max 5 offsets
    return jsonify(analytics.ghost_frontlines(ds, date_str, offsets))


# ── Time-to-city projections ─────────────────────────────

@app.route("/api/projections/<date_str>")
def api_projections(date_str):
    """Estimated days until frontline reaches key cities."""
    lookback = request.args.get("lookback", 30, type=int)
    lookback = min(max(lookback, 7), 180)
    return jsonify(analytics.time_to_city(ds, date_str, lookback))


# ── Salient detection ────────────────────────────────────

@app.route("/api/salients/<date_str>")
def api_salients(date_str):
    """Detect vulnerable salients/bulges in the frontline."""
    geom = ds.fetch_date(date_str)
    if geom:
        min_area = request.args.get("min_area", 5.0, type=float)
        return jsonify(analytics.detect_salients(geom, min_area))
    return jsonify({"error": f"No data for {date_str}"}), 404


# ── Air raid alerts ───────────────────────────────────────

@app.route("/api/alerts")
def api_alerts():
    """Real-time air raid alert status per oblast. Cached 30s."""
    return jsonify(raids.fetch_alerts())


# ── Weather ──────────────────────────────────────────────

@app.route("/api/weather")
def api_weather():
    """Current weather at key frontline locations. Cached 30min."""
    return jsonify(weather.fetch_weather())


# ── Export ───────────────────────────────────────────────

@app.route("/api/export/csv")
def api_export_csv():
    """Export time series as CSV. ?source=ds|divgen"""
    source = request.args.get("source", "ds")
    with _ts_lock:
        data = list(_ts_divgen if source == "divgen" else _ts_ds)
    if not data:
        return jsonify({"error": "No data"}), 503

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=[
        "date", "area_km2", "frontline_km", "polygons",
        "delta_km2", "rate_km2_per_day", "rate_7d_avg",
    ])
    writer.writeheader()
    for row in data:
        writer.writerow({k: row.get(k, "") for k in writer.fieldnames})

    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=ukraine_frontline_data.csv"},
    )


@app.route("/api/export/json")
def api_export_json():
    """Export time series as JSON. ?source=ds|divgen"""
    source = request.args.get("source", "ds")
    with _ts_lock:
        data = list(_ts_divgen if source == "divgen" else _ts_ds)
    if not data:
        return jsonify({"error": "No data"}), 503
    return Response(
        json.dumps(data, indent=2),
        mimetype="application/json",
        headers={"Content-Disposition": "attachment; filename=ukraine_frontline_data.json"},
    )


if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("  Ukraine Frontline Tracker")
    print("  http://localhost:5001")
    print("=" * 60)
    print("  Building time series in background...\n")

    # Start background pre-cache
    t = threading.Thread(target=_precache_time_series, daemon=True)
    t.start()

    import os
    port = int(os.environ.get("PORT", 5001))
    app.run(host="0.0.0.0", port=port, debug=True, use_reloader=False)
