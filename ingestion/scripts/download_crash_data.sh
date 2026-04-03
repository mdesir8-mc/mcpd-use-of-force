#!/usr/bin/env bash
# Download Montgomery County crash/CAD data from the Socrata open data portal.
# Output: data/raw/crash_data.csv

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
OUTPUT="${REPO_ROOT}/data/raw/crash_data.csv"

URL="https://data.montgomerycountymd.gov/resource/98cc-bc7d.csv?\$query=SELECT%0A%20%20%60incident_id%60%2C%0A%20%20%60cr_number%60%2C%0A%20%20%60crash_reports%60%2C%0A%20%20%60start_time%60%2C%0A%20%20%60end_time%60%2C%0A%20%20%60priority%60%2C%0A%20%20%60initial_type%60%2C%0A%20%20%60close_type%60%2C%0A%20%20%60address%60%2C%0A%20%20%60city%60%2C%0A%20%20%60state%60%2C%0A%20%20%60zip%60%2C%0A%20%20%60longitude%60%2C%0A%20%20%60latitude%60%2C%0A%20%20%60police_district_number%60%2C%0A%20%20%60sector%60%2C%0A%20%20%60pra%60%2C%0A%20%20%60calltime_callroute%60%2C%0A%20%20%60calltime_dispatch%60%2C%0A%20%20%60calltime_arrive%60%2C%0A%20%20%60calltime_cleared%60%2C%0A%20%20%60callroute_dispatch%60%2C%0A%20%20%60dispatch_arrive%60%2C%0A%20%20%60arrive_cleared%60%2C%0A%20%20%60disposition_desc%60%2C%0A%20%20%60geolocation%60%0AWHERE%0A%20%20(%60start_time%60%20%3E%20%222022-01-01T00%3A00%3A00%22%20%3A%3A%20floating_timestamp)%0A%20%20AND%20%60cr_number%60%20IS%20NOT%20NULL%0AORDER%20BY%20%60end_time%60%20DESC%20NULL%20FIRST%2C%20%60start_time%60%20ASC%20NULL%20LAST&\$limit=9999999"

echo "Downloading crash data to ${OUTPUT} ..."
curl -fSL --retry 3 --retry-delay 5 -o "${OUTPUT}" "${URL}"
echo "Done. $(wc -l < "${OUTPUT}") lines written."
