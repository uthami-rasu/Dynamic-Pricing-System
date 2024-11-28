#!/bin/bash

# Set the path to your PySpark environment, if necessary
# Example: export PYSPARK_HOME=/path/to/spark
source ~/pyspark-razz/pyspark-env/bin/activate

# Activate any necessary virtual environment if you are using one
# source /path/to/venv/bin/activate

# Run the Python ETL script
python3 /home/razz/DE/StreamProject/StreamProcessing.py

