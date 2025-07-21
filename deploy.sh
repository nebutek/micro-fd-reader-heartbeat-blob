#!/bin/bash

# Azure configuration
ACR_NAME="betacontainers"
RESOURCE_GROUP="aks-dev-rg"
CLUSTER_NAME="aks-dev-cluster"
IMAGE_NAME="fd-reader-heartbeat-blob"
IMAGE_TAG="latest"
NAMESPACE="fleetdefender"

# Check if user is logged into Azure
if ! az account show >/dev/null 2>&1; then
    echo "Not logged into Azure. Initiating login..."
    az login
else
    echo "Already logged into Azure"
fi

# Login to Azure Container Registry
az acr login --name $ACR_NAME

# Build the Docker image - ensure your Dockerfile has FROM --platform=linux/amd64
echo "Building Docker image..."
docker build -t $IMAGE_NAME:$IMAGE_TAG .

# Tag the image for ACR
docker tag $IMAGE_NAME:$IMAGE_TAG $ACR_NAME.azurecr.io/$IMAGE_NAME:$IMAGE_TAG

# Push the image to ACR
docker push $ACR_NAME.azurecr.io/$IMAGE_NAME:$IMAGE_TAG

# Deploy to Kubernetes
echo "Deploying to Kubernetes..."
kubectl apply -f aks/manifests/deployment.yaml

# Force a rolling update to pick up the new image
echo "Triggering rolling update..."
kubectl rollout restart deployment/$IMAGE_NAME -n fleetdefender

# Wait for the rollout to complete
echo "Waiting for rollout to complete..."
kubectl rollout status deployment/$IMAGE_NAME -n fleetdefender

echo "Deployment completed successfully!"
echo "Image: $ACR_NAME.azurecr.io/$IMAGE_NAME:$IMAGE_TAG"
echo "Namespace: fleetdefender"
echo "Deployment: $IMAGE_NAME"

# Show the pods
echo "Current pods:"
kubectl get pods -n fleetdefender -l app=$Image
