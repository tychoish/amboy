#!/usr/bin/env bash

# Run tests that don't require an external db.

make test-pool \
     test-registry \
     test-job \
     test-management \
     test-rest
