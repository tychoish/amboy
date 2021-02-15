#!/usr/bin/env bash

# Run tests that require a postgres db.

set -e

pushd "$(git rev-parse --show-toplevel)"
# Postgres requires that a password is set, but we don't need to provide one if
# host auth method is set to trust.
docker run --name amboy-postgres \
       -p 5432:5432 \
       -e POSTGRES_USER=amboy \
       -e POSTGRES_PASSWORD=password \
       -e POSTGRES_HOST_AUTH_METHOD=trust \
       -d postgres \
       -c max_connections=500

# Wait for postgres to start accepting connections.
max_attempts=10
curr_attempt=1
until pg_isready --host localhost --port 5432; do
    echo "Waiting for postgres"
    if (( curr_attempt > max_attempts )); then
        echo "Number of attempts to connect to postgres exceeded max, exiting..."
        exit 1
    fi
    ((curr_attempt++))
    sleep 2
done

# make coverage-queue-pgq

go test ./queue/pgq

docker rm -f amboy-postgres || true
popd
