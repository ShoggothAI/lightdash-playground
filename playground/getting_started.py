#!/usr/bin/env python
# Getting started with Lightdash
# This script contains the same code as in getting_started.ipynb

import os
from dotenv import load_dotenv

# Load environment variables
dotenv_path = os.path.realpath("../../lightdash-playground/.env")
print(f"Loading environment from: {dotenv_path}")
assert load_dotenv(dotenv_path), "Couldn't load the .env file"

# Import Lightdash
import lightdash
from lightdash import Client
from lightdash.filter import DimensionFilter

# Setup Lightdash Client
access_token = os.getenv("LIGHTDASH_ACCESS_TOKEN")
project_uuid = os.getenv("LIGHTDASH_PROJECT_UUID")
instance_url = os.getenv("LIGHTDASH_INSTANCE_URL")

client = Client(instance_url=instance_url, access_token=access_token, project_uuid=project_uuid)

# List all models
print("Available models:")
models = client.list_models()
print(models)

# Get a specific model
ts_data = client.models.time_series_data
print(f"\nSelected model: {ts_data}")

# Find available metrics
print("\nAvailable metrics:")
metric = ts_data.list_metrics()[1]
print(f"First metric: {metric}")
print(f"Metric description: {metric.description}")

# Run a simple query
print("\nRunning a simple query:")
result = ts_data.query(metrics=[metric]).to_records()
print(result)

# List available dimensions
print("\nAvailable dimensions:")
dim = ts_data.list_dimensions()[0]
print(f"First dimension: {dim}")

# Query with dimensions
print("\nRunning a query with dimensions:")
df = ts_data.query(dimensions=[dim], metrics=[metric]).to_df()
print(df)

values = df[dim.label].values
print(values)

filter = DimensionFilter(ts_data.list_dimensions()[0], "equals", [values[0]])

test = filter.to_dict()

df2 = ts_data.query(
    dimensions=[ts_data.list_dimensions()[0], ts_data.list_dimensions()[2]],
    metrics=[metric],
    filters=filter,
).to_df()

print(df2)
print("yay!")
