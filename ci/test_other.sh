#!/usr/bin/env bash

# Run tests that don't require an external db.

make coverage-pool \
     coverage-registry \
     coverage-job \
     coverage-management \
     coverage-rest
