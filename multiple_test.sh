#!/bin/bash

# Run this after every feature to ensure to false positives
# or race conditions that are not caught on a single run

num_runs=5
for ((i = 1; i <= $num_runs; i++))
do
  if go clean -testcache ; then
    # Run tests
    if go test -race -v ./... ; then
      echo ""
    else
      echo "Test run $i: One of the tests failed. Exiting!"
      exit 1
    fi
  else
    echo "Error cleaning test cache. Exiting!"
    exit 1
  fi

done