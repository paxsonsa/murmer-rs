#!/bin/sh
# Entrypoint for the cluster_chat Docker demo.
#
# Makes a key-based murmer cluster "just work" with `docker compose up`, with no
# manual key generation. Each node:
#   1. generates (or loads) its own persistent key in the shared /keys volume,
#   2. publishes its PUBLIC endpoint id to /keys/<node>.id,
#   3. if it is a joiner, waits for the seed's published id and dials it by key.
#
# Config via environment:
#   MURMER_NODE  required  this node's name (also its key/id filenames)
#   MURMER_SEED  optional  name of the seed node to join; empty = this is the seed
#   MURMER_PORT  optional  listen port (default 7100)
set -eu

NODE="${MURMER_NODE:?MURMER_NODE must be set}"
SEED="${MURMER_SEED:-}"
PORT="${MURMER_PORT:-7100}"
KEYDIR="/keys"

mkdir -p "$KEYDIR"
KEY="$KEYDIR/$NODE.key"

# This container's address on the Docker network, so peers can reach it (the
# default loopback advertise would not be reachable from other containers).
MY_IP="$(getent hosts "$NODE" | awk '{print $1; exit}')"
[ -z "$MY_IP" ] && MY_IP="$(hostname -i | awk '{print $1}')"
ADVERTISE="$MY_IP:$PORT"

# Generate or load this node's key and publish its public endpoint id. Only the
# public id is written to the shared .id file; the secret stays in the .key file.
MY_ID="$(murmer id --key "$KEY")"
printf '%s\n' "$MY_ID" > "$KEYDIR/$NODE.id"
echo "[$NODE] endpoint id: $MY_ID  advertising $ADVERTISE"

if [ -z "$SEED" ]; then
    echo "[$NODE] starting as seed node on :$PORT"
    exec cluster_chat --node "$NODE" --port "$PORT" --key "$KEY" \
        --advertise "$ADVERTISE"
fi

# Joiner: wait for the seed to publish its id, then dial it by key.
SEED_ID_FILE="$KEYDIR/$SEED.id"
echo "[$NODE] waiting for seed '$SEED' to publish its id..."
while [ ! -s "$SEED_ID_FILE" ]; do
    sleep 0.5
done
SEED_ID="$(cat "$SEED_ID_FILE")"
echo "[$NODE] joining via $SEED_ID@$SEED:$PORT"
exec cluster_chat --node "$NODE" --port "$PORT" --key "$KEY" \
    --advertise "$ADVERTISE" --seed "$SEED_ID@$SEED:$PORT"
