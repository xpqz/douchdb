#!/usr/bin/env python3
"""Install the explorer's design documents (_design/explore) in the four
aviation databases imported by scripts/import_kaggle.py.

Every grouping view emits a numeric "fatalities" value and reduces with
_stats, so one query gives both the number of events (count) and the
fatalities (sum) per key. Keys are upper-cased and trimmed so spellings such
as "Cessna" and "CESSNA" group together.

Usage: DOUCHDB_PASSWORD=secret python3 views.py [--url http://127.0.0.1:5984]
"""
import argparse, base64, json, os, urllib.request, urllib.error

# Shared helpers, inlined into every map function (QuickJS, ES5 style).
HELPERS = r"""
  function norm(s) { return (s === null || s === undefined || s === '') ? '(unknown)' : String(s).replace(/\s+/g, ' ').trim().toUpperCase(); }
  function num(n) { return (typeof n === 'number' && n === n) ? n : 0; }
  function year(d) { return (typeof d === 'string' && d.length >= 4) ? parseInt(d.substring(0, 4), 10) : null; }
"""

def grouped(key_expr, value_expr, guard="true"):
    return {
        "map": "function (doc) {" + HELPERS +
               "  var y = year(doc.event_date);\n"
               "  if (!(" + guard + ")) return;\n"
               "  emit(" + key_expr + ", " + value_expr + ");\n}",
        "reduce": "_stats",
    }

def lookup(key_expr, guard):
    return {"map": "function (doc) {" + HELPERS +
                   "  if (!(" + guard + ")) return;\n  emit(" + key_expr + ", null);\n}"}

FATAL = {
    "airline_accidents": "num(doc.total_fatal_injuries)",
    "faa_incidents_data": "num(doc.total_fatalities)",
    "ntsb_aviation_data": "(doc.injury_severity === 'FATAL' ? 1 : 0)",
    "world_aircraft_accident_summary": "num(doc.crew_fatalities) + num(doc.pax_fatalities)",
}

# label -> key expression, per dataset. Each also gets a [key, year] twin
# ("<name>_year") so a chosen value can be shown over time.
DIMENSIONS = {
    "airline_accidents": {
        "make": "norm(doc.aircraft_make)", "phase": "norm(doc.broad_phase_of_flight)",
        "weather": "norm(doc.weather_condition)", "country": "norm(doc.country)",
        "purpose": "norm(doc.purpose_of_flight)", "damage": "norm(doc.aircraft_damage)",
        "engine": "norm(doc.engine_type)", "category": "norm(doc.aircraft_category)",
    },
    "faa_incidents_data": {
        "make": "norm(doc.aircraft_make)", "phase": "norm(doc.flight_phase)",
        "state": "norm(doc.event_state)", "flight_type": "norm(doc.primary_flight_type)",
        "damage": "norm(doc.aircraft_damage)", "pilot": "norm(doc.pic_certificate_type)",
    },
    "ntsb_aviation_data": {
        "make": "norm(doc.aircraft_make)", "phase": "norm(doc.flight_phase)",
        "state": "norm(doc.event_state)", "severity": "norm(doc.injury_severity)",
        "conduct": "norm(doc.flight_conduct)", "operator": "norm(doc.operator)",
    },
    "world_aircraft_accident_summary": {
        "operator": "norm(doc.operator)",
        "aircraft": "norm(String(doc.aircraft || '').split(' ')[0])",
    },
}

def design(db):
    fatal = FATAL[db]
    views = {
        # [year, month]: group_level=1 for years, 2 for months.
        "by_date": grouped("[y, parseInt(doc.event_date.substring(5, 7), 10)]", fatal, "y !== null"),
        # Every event keyed by its fatalities, for "deadliest" (descending=true).
        "by_fatalities": lookup(fatal, fatal + " > 0"),
        # Event lookups by registration and by event id.
        "by_registration": lookup("norm(doc.aircraft_registration)", "doc.aircraft_registration"),
    }
    for name, key in DIMENSIONS[db].items():
        views["by_" + name] = grouped(key, fatal)
        views["by_" + name + "_year"] = grouped("[" + key + ", y]", fatal, "y !== null")
    if db == "airline_accidents":
        # One-degree grid cells for the map.
        views["by_place"] = grouped(
            "[Math.round(doc.latitude), Math.round(doc.longitude)]", fatal,
            "typeof doc.latitude === 'number' && typeof doc.longitude === 'number'")
    return {"_id": "_design/explore", "language": "javascript", "views": views,
            "dimensions": list(DIMENSIONS[db])}

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--url", default="http://127.0.0.1:5984")
    p.add_argument("--username", default="admin")
    a = p.parse_args()
    auth = "Basic " + base64.b64encode(f"{a.username}:{os.environ['DOUCHDB_PASSWORD']}".encode()).decode()
    def req(method, path, body=None):
        r = urllib.request.Request(a.url + path, method=method, headers={"Authorization": auth, "Content-Type": "application/json"},
                                   data=None if body is None else json.dumps(body).encode())
        try:
            with urllib.request.urlopen(r, timeout=120) as resp:
                return resp.status, json.load(resp)
        except urllib.error.HTTPError as e:
            return e.code, json.load(e)
    for db in DESIGN_DBS:
        doc = design(db)
        status, current = req("GET", f"/{db}/_design/explore")
        if status == 200:
            doc["_rev"] = current["_rev"]
        status, body = req("PUT", f"/{db}/_design/explore", doc)
        print(db, status, body.get("rev") or body)

DESIGN_DBS = list(FATAL)
if __name__ == "__main__":
    main()
