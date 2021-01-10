#!/bin/bash


### Main variables

# User ID on GCP
export GCP_userID="XXXX"

# Private key to use to connect to GCP
export GCP_privateKeyFile="XXXX"

# Name of your GCP project
export TF_VAR_project="XXXX"

# Name of your selected GCP region
export TF_VAR_region="us-central1"

# Name of your selected GCP zone
export TF_VAR_zone="us-central1-b"



### Other variables used by Terrform

# Number of VMs created
export TF_VAR_machineCount=2

# VM type
export TF_VAR_machineType="f1-micro"

# Prefix for you VM instances
export TF_VAR_instanceName="tf-instance"

# Prefix of your GCP deployment key
export TF_VAR_deployKeyName="deployment_key.json"
