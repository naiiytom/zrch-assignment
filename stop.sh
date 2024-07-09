echo "Stopping"
docker compose down --remove-orphans -v
rm -rf ./db
echo "Done Stopping"