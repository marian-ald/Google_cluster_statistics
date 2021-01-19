#!/bin/bash


### Main variables

# User ID on GCP
export GCP_userID="ghaffarian_pardis_gmail_com"

# Private key to use to connect to GCP
export GCP_privateKeyFile="~/.ssh/id_ed25519"

# Name of your GCP project
export TF_VAR_project="solid-garden-302208"

# Name of your selected GCP region
export TF_VAR_region="us-central1"

# Name of your selected GCP zone
export TF_VAR_zone="us-central1-b"



### Other variables used by Terrform

# Number of VMs created
export TF_VAR_machineCount=1

# VM type
export TF_VAR_machineType="n2-highmem-8"

# Prefix for you VM instances
export TF_VAR_instanceName="tf-instance"

# Prefix of your GCP deployment key
export TF_VAR_deployKeyName="deployment-key-spark.json"
