#!/bin/bash

echo "dckr_pat_khJf2c2u70Ig6SfZEQVeq4qb1ig" | docker login -u wdevzurc --password-stdin

docker build -t jd/airflow .

docker-compose up airflow-init
docker-compose up -d
