#!/usr/bin/env bash
set -euo pipefail

# Several editors type into one live doc at once over real WebSockets while
# their connections are cut at random and the server is killed and restarted.
# The run fails unless every editor ends with exactly the server's text.
# Tune with CLIENTS, DURATION (seconds), and SEED.
cd "$(dirname "$0")/.."
node internal/atlas/uitests/e2e.js
