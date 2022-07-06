#!/bin/bash

# CREATE A .env.sh FILE THAT EXPORTS AWS KEY, SECRET & REGION
# UPDATE THE ARGUMENTS AS REQUIRED
source .env.sh 

spark-submit app.py --config '{ 
        "yaml_bucket": "cfa-demo-config", 
        "yaml_key": "DQB-config-DRAFT.yaml", 
        "source_bucket": "cfa-demo-pipeline2", 
        "source_key": "glue/DQB-POC0.csv",
        "content_length": 1,
        "version": 1
        }'


