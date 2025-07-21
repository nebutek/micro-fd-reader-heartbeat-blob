#!/bin/bash

# Default variables - change these to match your AKS cluster
ENVIRONMENT=${1:-"dev"}  # Can be "dev", "staging", or "prod"
RESOURCE_GROUP="aks-${ENVIRONMENT}-rg"
CLUSTER_NAME="aks-${ENVIRONMENT}-cluster"
CONTEXT_NAME="aks-${ENVIRONMENT}"

# Azure configuration
ACR_NAME="betacontainers"
IMAGE_NAME="fd-reader-heartbeat-blob"
NAMESPACE="fleetdefender"  # Change this if you have a specific namespace

# Check if user is logged into Azure
if ! az account show >/dev/null 2>&1; then
    echo "Not logged into Azure. Initiating login..."
    az login
else
    echo "Already logged into Azure"
fi

# Get AKS credentials and set context
echo "Getting AKS credentials for cluster ${CLUSTER_NAME}"
az aks get-credentials --resource-group $RESOURCE_GROUP --name $CLUSTER_NAME --overwrite-existing

# Apply Kubernetes manifests
echo "Applying Kubernetes manifests..."
kubectl apply -f aks/manifests/deployment.yaml

# Wait for deployment to roll out
echo "Waiting for deployment to complete..."
kubectl rollout status deployment/$IMAGE_NAME -n $NAMESPACE

# Get the service URL
echo "Getting service details..."
EXTERNAL_IP=$(kubectl get service $IMAGE_NAME -n $NAMESPACE -o jsonpath='{.status.loadBalancer.ingress[0].ip}')
PORT=$(kubectl get service $IMAGE_NAME -n $NAMESPACE -o jsonpath='{.spec.ports[0].port}')

echo "Application deployed at: http://$EXTERNAL_IP:$PORT" 