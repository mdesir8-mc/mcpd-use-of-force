#!/usr/bin/env bash
# Download Montgomery County crash/CAD data from the Socrata open data portal.
# Output: data/raw/raw_data.csv

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
OUTPUT="${REPO_ROOT}/data/raw/raw_data.csv"

echo "Downloading raw data to ${OUTPUT} ..."

curl -fSL --retry 3 --retry-delay 5 -G \
  --data-urlencode '$select=incident_id,cr_number,crash_reports,start_time,end_time,priority,initial_type,close_type,address,city,state,zip,longitude,latitude,police_district_number,sector,pra,calltime_callroute,calltime_dispatch,calltime_arrive,calltime_cleared,callroute_dispatch,dispatch_arrive,arrive_cleared,disposition_desc,geolocation' \
  --data-urlencode '$where=start_time > "2022-01-01T00:00:00" AND cr_number IS NOT NULL' \
  --data-urlencode '$order=end_time DESC NULL FIRST, start_time ASC NULL LAST' \
  --data-urlencode '$limit=9999999' \
  -o "${OUTPUT}" \
  "https://data.montgomerycountymd.gov/resource/98cc-bc7d.csv"

echo "Done. $(wc -l < "${OUTPUT}") lines written."
