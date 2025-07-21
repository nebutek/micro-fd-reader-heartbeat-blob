#!/usr/bin/env python3
import subprocess
import sys

NAMESPACE = "fleetdefender"
DEPLOYMENT = "fd-reader-heartbeat-blob"

def get_pods(deployment, namespace):
    # Get pods with label app=<deployment>
    cmd = [
        "kubectl", "get", "pods",
        "-n", namespace,
        "-l", f"app={deployment}",
        "-o", "jsonpath={.items[*].metadata.name}"
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    pods = result.stdout.strip().split()
    return pods

def get_logs(pod, namespace):
    cmd = [
        "kubectl", "logs", pod,
        "-n", namespace
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    return result.stdout

def main():
    pods = get_pods(DEPLOYMENT, NAMESPACE)
    if not pods:
        print("No pods found for deployment:", DEPLOYMENT)
        return
    for pod in pods:
        print(f"\n--- Logs for pod: {pod} ---\n")
        logs = get_logs(pod, NAMESPACE)
        print(logs)

if __name__ == "__main__":
    main()