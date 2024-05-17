#!/bin/bash
echo "-------------------- UPDATER --------------------"

mvn clean package -f "../.deployment/updater/pom.xml"

echo "Checking for existing updater image..."
imageFullName="pgfacade-updater:latest"
imageExists=$(docker images --format "{{.Repository}}:{{.Tag}}" "$imageFullName")
if [[ -n "$imageExists" ]]; then
  echo "Deleting image $imageFullName for building new version..."
  docker rmi "$imageFullName"
fi

echo "Building new updater image..."
docker build --tag "pgfacade-updater" "../.deployment/updater"