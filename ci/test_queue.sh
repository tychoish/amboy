#!/usr/bin/env bash

# Run tests that don't require an external db.

RACE_DETECTOR=true make test-queue
