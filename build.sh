#!/bin/bash

echo "null" | docker login -u null --password-stdin

docker build -t jd/airflow .

docker-compose up airflow-init
docker-compose up -d
