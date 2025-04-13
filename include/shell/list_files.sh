#!/bin/bash

echo "The current user is $(whoami)"
files=${AIRFLOW_HOME}/dags/*
for file in ${files}; do
    echo "Found Files: $(basename $file)"
done
