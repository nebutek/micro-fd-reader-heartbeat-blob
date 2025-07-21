#!/bin/bash

# Check if environment parameter is provided
if [ $# -ne 1 ]; then
    echo "Error: Environment parameter is required"
    echo "Usage: $0 [dev|prod|local]"
    exit 1
fi

# Set environment
ENV=$1

# Validate environment parameter
if [[ "$ENV" != "dev" && "$ENV" != "prod" && "$ENV" != "local" ]]; then
    echo "Error: Invalid environment. Must be 'dev', 'prod', or 'local'"
    exit 1
fi

# Map environments to kubectl contexts
case $ENV in
    dev)
        CONTEXT="aks-dev"
        ;;
    prod)
        CONTEXT="aks-prod"
        ;;
    local)
        CONTEXT="k3d-telematics-local"
        ;;
esac

# Set variables
NAMESPACE="fleetdefender"
DEPLOYMENT_NAME="fd-reader-heartbeat-blob"

# Check current context and switch if needed
CURRENT_CONTEXT=$(kubectl config current-context)
if [ "$CURRENT_CONTEXT" != "$CONTEXT" ]; then
    echo "Switching kubectl context from $CURRENT_CONTEXT to $CONTEXT..."
    kubectl config use-context $CONTEXT
    if [ $? -ne 0 ]; then
        echo "Error: Failed to switch kubectl context to $CONTEXT"
        exit 1
    fi
fi

# Confirm before proceeding
echo "Environment: $ENV"
echo "Kubectl Context: $CONTEXT"
echo "Namespace: $NAMESPACE"
echo "Deployment: $DEPLOYMENT_NAME"
read -p "Do you want to proceed with deployment? (y/n): " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Deployment cancelled."
    exit 1
fi

# Print information
echo "Applying Kubernetes manifests for $DEPLOYMENT_NAME in namespace $NAMESPACE..."

# Apply deployment
echo "Applying deployment..."
kubectl apply -f aks/manifests/deployment.yaml

# Apply service
echo "Applying service..."
kubectl apply -f aks/manifests/service.yaml

# Apply ingress
echo "Applying ingress..."
kubectl apply -f aks/manifests/ingress.yaml

# Wait for the deployment to roll out
echo "Waiting for deployment to complete..."
kubectl rollout status deployment/$DEPLOYMENT_NAME -n $NAMESPACE

# Check the pod status
echo "Checking pod status..."
kubectl get pods -n $NAMESPACE -l app=$DEPLOYMENT_NAME

# Check service endpoints
echo "Checking service endpoints..."
kubectl get endpoints -n $NAMESPACE $DEPLOYMENT_NAME

# Show ingress information
echo "Checking ingress status..."
kubectl get ingress -n $NAMESPACE

echo "Deployment completed. Your application should now be accessible via:"
echo "- Service: $(kubectl get service $DEPLOYMENT_NAME -n $NAMESPACE -o jsonpath='{.status.loadBalancer.ingress[0].ip}'):8000"
echo "- Ingress: http://<ingress-ip>/fd-reader-heartbeat-blob/"
echo ""
echo "Prometheus metrics should be accessible at port 9090/metrics path."