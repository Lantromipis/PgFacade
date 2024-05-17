#!/bin/bash
echo "-------------------- PGFACADE --------------------"

mvn clean package -f "../pom.xml"

echo "Checking for existing pgfacade image..."
imageFullName="pgfacade-pgfacade:latest"
imageExists=$(docker images --format "{{.Repository}}:{{.Tag}}" "$imageFullName")
if [[ -n "$imageExists" ]]; then
  echo "Deleting image $imageFullName for building new version..."
  docker rmi "$imageFullName"
fi

echo "Building new pgfacade image..."
docker build --tag "pgfacade-pgfacade" -f "../.deployment/Dockerfile.jvm" "../app-pgfacade-root"