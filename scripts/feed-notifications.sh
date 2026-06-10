#!/usr/bin/env bash
# Feed the notification API with test notifications (default: 20).
set -euo pipefail

DEFAULT_URL="http://localhost:8080"
DEFAULT_COUNT=20
DEFAULT_MODE="sequential"

TOKEN="${TOKEN:-}"
BASE_URL="$DEFAULT_URL"
COUNT="$DEFAULT_COUNT"
MODE="$DEFAULT_MODE"
DELAY_MS=0

usage() {
  cat <<'EOF'
Usage: feed-notifications.sh -t TOKEN [options]

Submit test notifications to the notification API.

Required:
  -t, --token TOKEN     Bearer JWT (or set TOKEN env var)

Options:
  -u, --url URL         API base URL (default: http://localhost:8080)
  -n, --count N         Number of notifications (default: 20)
  -m, --mode MODE       sequential | batch (default: sequential)
  -d, --delay MS        Delay between sequential requests in ms (default: 0)
  -h, --help            Show this help

Examples:
  export TOKEN=$(make gentoken user_id=demo-user)
  ./scripts/feed-notifications.sh -t "$TOKEN"
  ./scripts/feed-notifications.sh -t "$TOKEN" -n 15 -m batch
  ./scripts/feed-notifications.sh -t "$TOKEN" -u http://localhost:8080 -n 20 -d 100
EOF
}

die() {
  echo "error: $*" >&2
  exit 1
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -t|--token)
      TOKEN="${2:-}"
      shift 2
      ;;
    -u|--url)
      BASE_URL="${2:-}"
      shift 2
      ;;
    -n|--count)
      COUNT="${2:-}"
      shift 2
      ;;
    -m|--mode)
      MODE="${2:-}"
      shift 2
      ;;
    -d|--delay)
      DELAY_MS="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      die "unknown argument: $1 (try --help)"
      ;;
  esac
done

[[ -n "$TOKEN" ]] || die "token is required (-t TOKEN or TOKEN env var)"
[[ "$COUNT" =~ ^[0-9]+$ && "$COUNT" -ge 1 ]] || die "count must be a positive integer"
[[ "$MODE" == "sequential" || "$MODE" == "batch" ]] || die "mode must be sequential or batch"
[[ "$DELAY_MS" =~ ^[0-9]+$ ]] || die "delay must be a non-negative integer (milliseconds)"

BASE_URL="${BASE_URL%/}"
RUN_ID="$(date +%s)-$$"

channel_for() {
  case $(( ($1 - 1) % 3 )) in
    0) echo "sms" ;;
    1) echo "email" ;;
    2) echo "push" ;;
  esac
}

priority_for() {
  case $(( ($1 - 1) % 3 )) in
    0) echo "high" ;;
    1) echo "normal" ;;
    2) echo "low" ;;
  esac
}

recipient_for() {
  local idx="$1"
  local channel="$2"
  case "$channel" in
    sms) printf '+1555%07d' "$idx" ;;
    email) printf 'user%d@example.com' "$idx" ;;
    push) printf 'device-token-%d' "$idx" ;;
  esac
}

content_for() {
  local idx="$1"
  local channel="$2"
  printf 'Load-test notification #%d via %s (run %s)' "$idx" "$channel" "$RUN_ID"
}

post_one() {
  local idx="$1"
  local channel priority recipient content idem_key body http_code response

  channel="$(channel_for "$idx")"
  priority="$(priority_for "$idx")"
  recipient="$(recipient_for "$idx" "$channel")"
  content="$(content_for "$idx" "$channel")"
  idem_key="feed-${RUN_ID}-${idx}"

  body="$(printf '{"recipient":"%s","channel":"%s","priority":"%s","content":"%s"}' \
    "$recipient" "$channel" "$priority" "$content")"

  response="$(curl -sS -w $'\n%{http_code}' -X POST "${BASE_URL}/api/v1/notifications" \
    -H "Authorization: Bearer ${TOKEN}" \
    -H "Content-Type: application/json" \
    -H "Idempotency-Key: ${idem_key}" \
    -d "$body")"

  http_code="${response##*$'\n'}"
  response="${response%$'\n'*}"

  if [[ "$http_code" == "202" || "$http_code" == "200" ]]; then
    if command -v jq >/dev/null 2>&1; then
      local id
      id="$(printf '%s' "$response" | jq -r '.id // empty')"
      echo "[$idx/$COUNT] OK ($http_code) id=${id:-?} channel=$channel priority=$priority"
    else
      echo "[$idx/$COUNT] OK ($http_code) channel=$channel priority=$priority"
    fi
    return 0
  fi

  echo "[$idx/$COUNT] FAIL ($http_code) channel=$channel priority=$priority" >&2
  echo "$response" >&2
  return 1
}

post_batch() {
  local batch_key="feed-batch-${RUN_ID}"
  local payload http_code response

  payload="$(python3 - "$COUNT" "$RUN_ID" <<'PY'
import json
import sys

count = int(sys.argv[1])
run_id = sys.argv[2]
channels = ["sms", "email", "push"]
priorities = ["high", "normal", "low"]

def recipient(i, channel):
    if channel == "sms":
        return f"+1555{i:07d}"
    if channel == "email":
        return f"user{i}@example.com"
    return f"device-token-{i}"

notifications = []
for i in range(1, count + 1):
    channel = channels[(i - 1) % 3]
    priority = priorities[(i - 1) % 3]
    notifications.append({
        "recipient": recipient(i, channel),
        "channel": channel,
        "priority": priority,
        "content": f"Load-test batch notification #{i} via {channel} (run {run_id})",
        "idempotency_key": f"feed-{run_id}-{i}",
    })

print(json.dumps({"notifications": notifications}))
PY
)"

  response="$(curl -sS -w $'\n%{http_code}' -X POST "${BASE_URL}/api/v1/notifications/batch" \
    -H "Authorization: Bearer ${TOKEN}" \
    -H "Content-Type: application/json" \
    -H "Idempotency-Key: ${batch_key}" \
    -d "$payload")"

  http_code="${response##*$'\n'}"
  response="${response%$'\n'*}"

  if [[ "$http_code" == "202" || "$http_code" == "200" ]]; then
    if command -v jq >/dev/null 2>&1; then
      echo "$response" | jq -r '"batch id=\(.id) created=\(.created_count) duplicate=\(.duplicate_count) total=\(.total_count)"'
    else
      echo "batch accepted (HTTP $http_code)"
      echo "$response"
    fi
    return 0
  fi

  echo "batch request failed (HTTP $http_code)" >&2
  echo "$response" >&2
  return 1
}

echo "Feeding $COUNT notification(s) to ${BASE_URL} (mode=$MODE, run=$RUN_ID)"

ok=0
fail=0

if [[ "$MODE" == "batch" ]]; then
  if post_batch; then
    ok=$COUNT
  else
    fail=$COUNT
  fi
else
  for i in $(seq 1 "$COUNT"); do
    if post_one "$i"; then
      ok=$((ok + 1))
    else
      fail=$((fail + 1))
    fi
    if [[ "$DELAY_MS" -gt 0 && "$i" -lt "$COUNT" ]]; then
      sleep "$(awk "BEGIN { printf \"%.3f\", $DELAY_MS / 1000 }")"
    fi
  done
fi

echo "Done: $ok succeeded, $fail failed"
[[ "$fail" -eq 0 ]]
