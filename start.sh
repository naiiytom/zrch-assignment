#!/bin/sh
echo "Start"
docker compose up postgres -d
sleep(10)
docker compose up python --build -d
echo "Complete"