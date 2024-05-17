#!/bin/bash
echo "-------------------- BALANCER --------------------"

mvn clean package -f "../.deployment/load-balancer/pom.xml"

echo "Checking for existing updater image..."
imageFullName="pgfacade-balancer:latest"
imageExists=$(docker images --format "{{.Repository}}:{{.Tag}}" "$imageFullName")
if [[ -n "$imageExists" ]]; then
  echo "Deleting image $imageFullName for building new version..."
  docker rmi "$imageFullName"
fi

echo "Building new balancer image..."
docker build --tag "pgfacade-balancer" -f "../.deployment/load-balancer/docker/Dockerfile.jvm" "../.deployment/load-balancer"
