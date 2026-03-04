#!/usr/bin/env python3
"""
Flume Water API client.

Usage:
    python flume_client_20260303.py [--dry-run] [--days N] [--units UNIT] [--no-save]
    python flume_client_20260303.py --monthly [--dry-run] [--no-save]

Options:
    --dry-run       Print requests without calling the API.
    --days N        Number of past days to query (default: 7).
    --units UNIT    LITERS, GALLONS, CUBIC_FEET, or CUBIC_METERS (default: LITERS).
    --no-save       Skip writing results to CSV.
    --monthly       Fetch full monthly totals (all available years) into water_usage_monthly.csv.
"""

import argparse
import csv
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import jwt
import requests
from dotenv import load_dotenv

BASE_URL = "https://api.flumewater.com"
TOKEN_CACHE_FILE = Path(__file__).parent / ".flume_token_cache.json"
ENV_FILE = Path(__file__).parent / ".env"
CSV_FILE = Path(__file__).parent / "water_usage.csv"
CSV_COLUMNS = ["date", "value_liters", "value_gallons", "device_id", "fetched_at"]
MONTHLY_CSV_FILE = Path(__file__).parent / "water_usage_monthly.csv"
MONTHLY_CSV_COLUMNS = ["year", "month", "month_name", "value_liters", "value_gallons", "device_id", "fetched_at"]
LITERS_TO_GALLONS = 0.264172
MONTH_NAMES = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]


# ---------------------------------------------------------------------------
# Credential loading
# ---------------------------------------------------------------------------

def load_credentials() -> dict:
    load_dotenv(ENV_FILE)
    required = ["FLUME_CLIENT_ID", "FLUME_CLIENT_SECRET", "FLUME_USERNAME", "FLUME_PASSWORD"]
    creds = {}
    missing = []
    for key in required:
        value = os.environ.get(key)
        if not value:
            missing.append(key)
        creds[key] = value
    if missing:
        print(f"[ERROR] Missing environment variables: {', '.join(missing)}")
        print(f"        Copy .env.example to .env and fill in your credentials.")
        sys.exit(1)
    return creds


# ---------------------------------------------------------------------------
# Token management
# ---------------------------------------------------------------------------

def _save_token_cache(data: dict) -> None:
    TOKEN_CACHE_FILE.write_text(json.dumps(data, indent=2))


def _load_token_cache() -> dict | None:
    if not TOKEN_CACHE_FILE.exists():
        return None
    try:
        return json.loads(TOKEN_CACHE_FILE.read_text())
    except (json.JSONDecodeError, OSError):
        return None


def _token_is_valid(access_token: str) -> bool:
    """Return True if the JWT has more than 60 seconds before expiry."""
    try:
        payload = jwt.decode(access_token, options={"verify_signature": False})
        exp = payload.get("exp", 0)
        return (exp - time.time()) > 60
    except Exception:
        return False


def decode_user_id(access_token: str) -> int:
    """Extract user_id from JWT payload without verifying the signature."""
    try:
        payload = jwt.decode(access_token, options={"verify_signature": False})
        return int(payload["user_id"])
    except (KeyError, ValueError, jwt.DecodeError) as exc:
        print(f"[ERROR] Could not decode user_id from token: {exc}")
        sys.exit(1)


def get_token(creds: dict, dry_run: bool = False) -> tuple[str, str]:
    """
    Return (access_token, refresh_token).
    Uses cache if the cached token is still valid; otherwise authenticates fresh.
    """
    cache = _load_token_cache()
    if cache and _token_is_valid(cache.get("access_token", "")):
        print("[AUTH] Using cached access token.")
        return cache["access_token"], cache["refresh_token"]

    url = f"{BASE_URL}/oauth/token"
    body = {
        "grant_type": "password",
        "client_id": creds["FLUME_CLIENT_ID"],
        "client_secret": creds["FLUME_CLIENT_SECRET"],
        "username": creds["FLUME_USERNAME"],
        "password": creds["FLUME_PASSWORD"],
    }

    if dry_run:
        print("[DRY-RUN] Would POST to:", url)
        print("[DRY-RUN] Body (secrets redacted):", json.dumps({
            **body,
            "client_secret": "***",
            "password": "***",
        }, indent=2))
        return ("dry_run_access_token", "dry_run_refresh_token")

    print("[AUTH] Requesting new access token...")
    resp = requests.post(url, json=body, timeout=15)
    resp.raise_for_status()
    data = resp.json()

    token_data = data["data"][0]
    access_token = token_data["access_token"]
    refresh_token = token_data["refresh_token"]

    _save_token_cache({"access_token": access_token, "refresh_token": refresh_token})
    print("[AUTH] Token obtained and cached.")
    return access_token, refresh_token


def refresh_access_token(creds: dict, refresh_token: str, dry_run: bool = False) -> str:
    """Exchange a refresh token for a new access token."""
    url = f"{BASE_URL}/oauth/token"
    body = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": creds["FLUME_CLIENT_ID"],
        "client_secret": creds["FLUME_CLIENT_SECRET"],
    }

    if dry_run:
        print("[DRY-RUN] Would POST token refresh to:", url)
        return "dry_run_access_token"

    resp = requests.post(url, json=body, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    token_data = data["data"][0]
    new_access = token_data["access_token"]
    _save_token_cache({"access_token": new_access, "refresh_token": refresh_token})
    print("[AUTH] Token refreshed.")
    return new_access


# ---------------------------------------------------------------------------
# Device discovery
# ---------------------------------------------------------------------------

def get_devices(user_id: int, access_token: str, dry_run: bool = False) -> list[dict]:
    """Return all devices registered to the user."""
    url = f"{BASE_URL}/users/{user_id}/devices"
    headers = {"Authorization": f"Bearer {access_token}"}

    if dry_run:
        print("[DRY-RUN] Would GET:", url)
        return [{"id": "dry_run_device_id", "type": 2, "location_name": "Dry Run Home"}]

    resp = requests.get(url, headers=headers, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    devices = data.get("data", [])
    print(f"[DEVICES] Found {len(devices)} device(s).")
    return devices


def get_primary_location(user_id: int, access_token: str, dry_run: bool = False) -> str:
    """Return the name of the primary location, or 'Unknown' if unavailable."""
    if dry_run:
        return "Dry Run Home"
    url = f"{BASE_URL}/users/{user_id}/locations"
    headers = {"Authorization": f"Bearer {access_token}"}
    try:
        resp = requests.get(url, headers=headers, timeout=15)
        resp.raise_for_status()
        locations = resp.json().get("data", [])
        primary = next((loc for loc in locations if loc.get("primary_location")), None)
        if primary:
            city = primary.get("city", "")
            state = primary.get("state", "")
            name = primary.get("name", "Home")
            return f"{name} — {city}, {state}" if city else name
    except Exception:
        pass
    return "Unknown"


def pick_meter(devices: list[dict]) -> dict:
    """Return the first device of type 2 (physical meter). Exit if none found."""
    meters = [d for d in devices if d.get("type") == 2]
    if not meters:
        print("[ERROR] No Flume meter (type=2) found on this account.")
        sys.exit(1)
    if len(meters) > 1:
        print(f"[DEVICES] Multiple meters found; using the first: {meters[0]['id']}")
    return meters[0]


# ---------------------------------------------------------------------------
# Water usage query
# ---------------------------------------------------------------------------

def query_usage(
    user_id: int,
    device_id: str,
    access_token: str,
    since: datetime,
    until: datetime,
    bucket: str = "DAY",
    units: str = "LITERS",
    dry_run: bool = False,
) -> list[dict]:
    """POST a water usage query and return the result rows."""
    url = f"{BASE_URL}/users/{user_id}/devices/{device_id}/query"
    headers = {"Authorization": f"Bearer {access_token}"}
    request_id = "daily_usage"

    body = {
        "queries": [
            {
                "request_id": request_id,
                "since_datetime": since.strftime("%Y-%m-%d %H:%M:%S"),
                "until_datetime": until.strftime("%Y-%m-%d %H:%M:%S"),
                "bucket": bucket,
                "units": units,
                "sort_direction": "ASC",
            }
        ]
    }

    if dry_run:
        print("[DRY-RUN] Would POST to:", url)
        print("[DRY-RUN] Query body:", json.dumps(body, indent=2))
        return [
            {"datetime": since.strftime("%Y-%m-%d %H:%M:%S"), "value": 42.0},
        ]

    resp = requests.post(url, headers=headers, json=body, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    rows = data["data"][0].get(request_id, [])
    return rows


# ---------------------------------------------------------------------------
# CSV persistence
# ---------------------------------------------------------------------------

def _load_existing_dates(device_id: str) -> set[str]:
    """Return the set of dates already stored in the CSV for this device."""
    if not CSV_FILE.exists():
        return set()
    existing = set()
    with CSV_FILE.open(newline="") as f:
        for row in csv.DictReader(f):
            if row.get("device_id") == str(device_id):
                existing.add(row["date"])
    return existing


def save_to_csv(rows: list[dict], device_id: str, dry_run: bool = False) -> int:
    """
    Append new rows to the CSV, skipping dates already present for this device.
    Returns the number of rows written.
    """
    existing_dates = _load_existing_dates(device_id)
    write_header = not CSV_FILE.exists()
    fetched_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    new_rows = []
    for row in rows:
        raw_dt = row.get("datetime", "")
        date = raw_dt[:10] if raw_dt else ""
        if not date or date in existing_dates:
            continue
        liters = round(row.get("value", 0.0), 4)
        new_rows.append({
            "date": date,
            "value_liters": liters,
            "value_gallons": round(liters * LITERS_TO_GALLONS, 4),
            "device_id": device_id,
            "fetched_at": fetched_at,
        })

    if dry_run:
        print(f"[DRY-RUN] Would write {len(new_rows)} new row(s) to {CSV_FILE.name} "
              f"(skipping {len(rows) - len(new_rows)} duplicate(s)).")
        return len(new_rows)

    if not new_rows:
        print(f"[CSV] No new rows to write — all dates already present.")
        return 0

    with CSV_FILE.open("a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        if write_header:
            writer.writeheader()
        writer.writerows(new_rows)

    print(f"[CSV] Wrote {len(new_rows)} new row(s) to {CSV_FILE.name} "
          f"(skipped {len(rows) - len(new_rows)} duplicate(s)).")
    return len(new_rows)


# ---------------------------------------------------------------------------
# Output formatting
# ---------------------------------------------------------------------------

def print_usage_table(rows: list[dict], units: str) -> None:
    unit_label = units.lower().replace("_", " ")
    print(f"\n{'Date':<22} {'Usage':>12}")
    print("-" * 36)
    total = 0.0
    for row in rows:
        dt = row.get("datetime", "N/A")
        val = row.get("value", 0.0)
        total += val
        print(f"{dt:<22} {val:>10.2f}  {unit_label}")
    print("-" * 36)
    print(f"{'Total':<22} {total:>10.2f}  {unit_label}\n")


# ---------------------------------------------------------------------------
# Monthly fetch + persistence
# ---------------------------------------------------------------------------

def fetch_monthly_year(
    user_id: int,
    device_id: str,
    access_token: str,
    year: int,
    dry_run: bool = False,
) -> list[dict]:
    """Fetch monthly totals (LITERS) for a full calendar year."""
    since = datetime(year, 1, 1, 0, 0, 0)
    until = datetime(year + 1, 1, 1, 0, 0, 0)
    url = f"{BASE_URL}/users/{user_id}/devices/{device_id}/query"
    headers = {"Authorization": f"Bearer {access_token}"}
    request_id = f"monthly_{year}"

    body = {
        "queries": [
            {
                "request_id": request_id,
                "since_datetime": since.strftime("%Y-%m-%d %H:%M:%S"),
                "until_datetime": until.strftime("%Y-%m-%d %H:%M:%S"),
                "bucket": "MON",
                "units": "LITERS",
                "sort_direction": "ASC",
            }
        ]
    }

    if dry_run:
        print(f"[DRY-RUN] Would POST monthly query for {year}")
        return [{"datetime": f"{year}-01-01 00:00:00", "value": 100.0}]

    resp = requests.post(url, headers=headers, json=body, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    return data["data"][0].get(request_id, [])


def save_monthly_csv(rows_by_year: dict[int, list[dict]], device_id: str, dry_run: bool = False) -> int:
    """
    Write monthly rows to water_usage_monthly.csv, skipping year+month combos already present.
    rows_by_year: {year: [{"datetime": ..., "value": ...}, ...]}
    """
    existing: set[tuple[str, str]] = set()
    if MONTHLY_CSV_FILE.exists():
        with MONTHLY_CSV_FILE.open(newline="") as f:
            for row in csv.DictReader(f):
                if row.get("device_id") == str(device_id):
                    existing.add((row["year"], row["month"]))

    write_header = not MONTHLY_CSV_FILE.exists()
    fetched_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    new_rows = []

    for year, rows in sorted(rows_by_year.items()):
        for row in rows:
            raw_dt = row.get("datetime", "")
            if not raw_dt:
                continue
            month = str(datetime.strptime(raw_dt[:10], "%Y-%m-%d").month)
            if (str(year), month) in existing:
                continue
            liters = round(row.get("value", 0.0), 4)
            new_rows.append({
                "year": year,
                "month": month,
                "month_name": MONTH_NAMES[int(month) - 1],
                "value_liters": liters,
                "value_gallons": round(liters * LITERS_TO_GALLONS, 4),
                "device_id": device_id,
                "fetched_at": fetched_at,
            })

    if dry_run:
        print(f"[DRY-RUN] Would write {len(new_rows)} new monthly row(s) to {MONTHLY_CSV_FILE.name}.")
        return len(new_rows)

    if not new_rows:
        print("[MONTHLY CSV] No new rows to write — all months already present.")
        return 0

    with MONTHLY_CSV_FILE.open("a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=MONTHLY_CSV_COLUMNS)
        if write_header:
            writer.writeheader()
        writer.writerows(new_rows)

    skipped = sum(len(r) for r in rows_by_year.values()) - len(new_rows)
    print(f"[MONTHLY CSV] Wrote {len(new_rows)} new row(s) to {MONTHLY_CSV_FILE.name} (skipped {skipped} duplicate(s)).")
    return len(new_rows)


def run_monthly(user_id: int, device_id: str, access_token: str, dry_run: bool = False) -> None:
    """Fetch all available monthly data from install year through current year."""
    current_year = datetime.now().year
    # Flume launched ~2018; cap backfill at 3 years to avoid hammering the API
    start_year = current_year - 2
    rows_by_year: dict[int, list[dict]] = {}

    for year in range(start_year, current_year + 1):
        print(f"[MONTHLY] Fetching {year}...")
        rows = fetch_monthly_year(user_id, device_id, access_token, year, dry_run=dry_run)
        non_zero = [r for r in rows if r.get("value", 0) > 0]
        print(f"[MONTHLY] {year}: {len(non_zero)} month(s) with data")
        if non_zero:
            rows_by_year[year] = non_zero

    if rows_by_year:
        save_monthly_csv(rows_by_year, device_id, dry_run=dry_run)
    else:
        print("[MONTHLY] No data returned for any year.")


# ---------------------------------------------------------------------------
# HTML chart generation
# ---------------------------------------------------------------------------

HTML_FILE = Path(__file__).parent / "index.html"


def _read_monthly_csv_as_json() -> str:
    """Read water_usage_monthly.csv and return it as a JSON string for embedding."""
    if not MONTHLY_CSV_FILE.exists():
        return "[]"
    rows = []
    with MONTHLY_CSV_FILE.open(newline="") as f:
        for row in csv.DictReader(f):
            rows.append({
                "year": row["year"],
                "month": row["month"],
                "month_name": row["month_name"],
                "value_liters": float(row["value_liters"]),
                "value_gallons": float(row["value_gallons"]),
            })
    return json.dumps(rows)


def generate_html(location: str = "Home", dry_run: bool = False) -> None:
    """Write index.html with monthly data embedded as inline JSON."""
    data_json = _read_monthly_csv_as_json()
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Home Water Usage — {location}</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.4/dist/chart.umd.min.js"></script>
  <style>
    *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
      background: #0f1117;
      color: #e0e0e0;
      min-height: 100vh;
      display: flex;
      flex-direction: column;
      align-items: center;
      padding: 2rem 1rem;
    }}
    header {{ text-align: center; margin-bottom: 2rem; }}
    header h1 {{ font-size: 1.5rem; font-weight: 600; color: #ffffff; letter-spacing: -0.02em; }}
    header p {{ font-size: 0.85rem; color: #888; margin-top: 0.35rem; }}
    .controls {{ display: flex; gap: 0.5rem; margin-bottom: 1.5rem; }}
    .toggle-btn {{
      padding: 0.4rem 1rem; border-radius: 999px; border: 1px solid #333;
      background: #1a1d27; color: #aaa; font-size: 0.8rem; cursor: pointer; transition: all 0.15s;
    }}
    .toggle-btn.active {{ background: #1A6BFF; border-color: #1A6BFF; color: #fff; }}
    .chart-wrap {{
      width: 100%; max-width: 900px; background: #1a1d27;
      border-radius: 12px; padding: 1.5rem; border: 1px solid #252836;
    }}
    canvas {{ width: 100% !important; }}
    .legend {{ display: flex; gap: 1.5rem; justify-content: center; margin-top: 1.25rem; flex-wrap: wrap; }}
    .legend-item {{ display: flex; align-items: center; gap: 0.4rem; font-size: 0.8rem; color: #bbb; }}
    .legend-dot {{ width: 10px; height: 10px; border-radius: 50%; }}
    footer {{ margin-top: 2rem; font-size: 0.75rem; color: #555; text-align: center; }}
    #status {{ font-size: 0.8rem; color: #666; margin-bottom: 1rem; }}
  </style>
</head>
<body>
  <header>
    <h1>Home Water Usage</h1>
    <p>{location} &mdash; Monthly totals by year</p>
  </header>
  <div class="controls">
    <button class="toggle-btn active" id="btn-gallons" onclick="setUnit('gallons')">Gallons</button>
    <button class="toggle-btn" id="btn-liters" onclick="setUnit('liters')">Liters</button>
  </div>
  <p id="status"></p>
  <div class="chart-wrap">
    <canvas id="chart" height="380"></canvas>
    <div class="legend" id="legend"></div>
  </div>
  <footer>Data source: Flume Water API &mdash; generated {generated_at}</footer>

  <script>
    const RAW_DATA = {data_json};
    const MONTHS = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];
    const COLORS = ["#1A6BFF","#88A943","#A80C7C","#DBAE06","#008C4A","#36A5CC"];

    let chartInstance = null;
    let currentUnit = "gallons";

    // Group by year -> month
    const allData = {{}};
    RAW_DATA.forEach(row => {{
      if (!allData[row.year]) allData[row.year] = {{}};
      allData[row.year][parseInt(row.month)] = row;
    }});

    const years = Object.keys(allData).sort();
    document.getElementById("status").textContent =
      RAW_DATA.length + " months of data across " + years.join(", ");

    function setUnit(unit) {{
      currentUnit = unit;
      document.getElementById("btn-gallons").classList.toggle("active", unit === "gallons");
      document.getElementById("btn-liters").classList.toggle("active", unit === "liters");
      renderChart();
    }}

    function buildDatasets(unit) {{
      return years.map((year, i) => {{
        const byMonth = allData[year];
        const data = MONTHS.map((_, mi) => {{
          const row = byMonth[mi + 1];
          if (!row) return null;
          return unit === "gallons" ? row.value_gallons : row.value_liters;
        }});
        const color = COLORS[i % COLORS.length];
        return {{
          label: year, data,
          borderColor: color, backgroundColor: color + "22",
          pointBackgroundColor: color, pointRadius: 4, pointHoverRadius: 6,
          borderWidth: 2, tension: 0.3, spanGaps: false,
        }};
      }});
    }}

    function renderChart() {{
      const unit = currentUnit;
      const label = unit === "gallons" ? "Gallons" : "Liters";
      const datasets = buildDatasets(unit);

      if (chartInstance) {{
        chartInstance.data.datasets = datasets;
        chartInstance.options.plugins.tooltip.callbacks.label = ctx =>
          ` ${{ctx.dataset.label}}: ${{ctx.parsed.y !== null
            ? ctx.parsed.y.toLocaleString(undefined, {{maximumFractionDigits: 0}})
            : "—"}} ${{label.toLowerCase()}}`;
        chartInstance.options.scales.y.title.text = label;
        chartInstance.update();
        return;
      }}

      const ctx = document.getElementById("chart").getContext("2d");
      chartInstance = new Chart(ctx, {{
        type: "line",
        data: {{ labels: MONTHS, datasets }},
        options: {{
          responsive: true,
          interaction: {{ mode: "index", intersect: false }},
          plugins: {{
            legend: {{ display: false }},
            tooltip: {{
              backgroundColor: "#1e2130", borderColor: "#333", borderWidth: 1,
              titleColor: "#eee", bodyColor: "#ccc",
              callbacks: {{
                label: ctx =>
                  ` ${{ctx.dataset.label}}: ${{ctx.parsed.y !== null
                    ? ctx.parsed.y.toLocaleString(undefined, {{maximumFractionDigits: 0}})
                    : "—"}} ${{label.toLowerCase()}}`
              }}
            }}
          }},
          scales: {{
            x: {{ grid: {{ color: "#252836" }}, ticks: {{ color: "#888" }} }},
            y: {{
              beginAtZero: true,
              grid: {{ color: "#252836" }},
              ticks: {{ color: "#888", callback: v => v.toLocaleString() }},
              title: {{ display: true, text: label, color: "#666", font: {{ size: 11 }} }}
            }}
          }}
        }}
      }});

      // Custom legend
      const legendEl = document.getElementById("legend");
      legendEl.innerHTML = "";
      years.forEach((year, i) => {{
        const color = COLORS[i % COLORS.length];
        legendEl.innerHTML +=
          `<div class="legend-item"><div class="legend-dot" style="background:${{color}}"></div><span>${{year}}</span></div>`;
      }});
    }}

    renderChart();
  </script>
</body>
</html>"""

    if dry_run:
        print(f"[DRY-RUN] Would write index.html with {len(json.loads(data_json))} embedded rows.")
        return

    HTML_FILE.write_text(html, encoding="utf-8")
    print(f"[HTML] index.html generated with {len(json.loads(data_json))} embedded rows.")


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Query Flume water meter usage.")
    parser.add_argument("--dry-run", action="store_true", help="Print requests without calling the API.")
    parser.add_argument("--days", type=int, default=7, help="Number of past days to query (default: 7).")
    parser.add_argument(
        "--units",
        choices=["LITERS", "GALLONS", "CUBIC_FEET", "CUBIC_METERS"],
        default="LITERS",
        help="Unit for water volume (default: LITERS).",
    )
    parser.add_argument("--no-save", action="store_true", help="Skip writing results to CSV.")
    parser.add_argument("--monthly", action="store_true", help="Fetch monthly totals for all available years.")
    parser.add_argument("--build-html", action="store_true", help="Regenerate index.html from existing CSV without hitting the API.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.dry_run:
        print("[MODE] Dry-run enabled — no API calls will be made.\n")

    if args.build_html:
        generate_html(dry_run=args.dry_run)
        return

    creds = load_credentials()

    access_token, refresh_token = get_token(creds, dry_run=args.dry_run)

    if not args.dry_run:
        user_id = decode_user_id(access_token)
    else:
        user_id = 0

    print(f"[INFO] User ID: {user_id}")

    devices = get_devices(user_id, access_token, dry_run=args.dry_run)
    meter = pick_meter(devices)
    device_id = meter["id"]
    location = get_primary_location(user_id, access_token, dry_run=args.dry_run)
    print(f"[INFO] Using meter: {device_id}  ({location})")

    if args.monthly:
        run_monthly(user_id, device_id, access_token, dry_run=args.dry_run)
        generate_html(location=location, dry_run=args.dry_run)
        return

    until = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    since = until - timedelta(days=args.days)

    print(f"[QUERY] Fetching daily usage from {since.date()} to {until.date()} in {args.units}...")
    rows = query_usage(
        user_id=user_id,
        device_id=device_id,
        access_token=access_token,
        since=since,
        until=until,
        units=args.units,
        dry_run=args.dry_run,
    )

    if not rows:
        print("[RESULT] No data returned for this period.")
    else:
        print_usage_table(rows, args.units)
        if not args.no_save:
            save_to_csv(rows, device_id, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
