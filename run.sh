#!/bin/bash

# CREATE A .env.sh FILE THAT EXPORTS AWS KEY, SECRET & REGION
# UPDATE THE ARGUMENTS AS REQUIRED
source .env.sh 

spark-submit src/test_spark_app.py --config '{
    "pipeline_config": {
        "run": "execute_dqb", 
        "pipeline_id": "pipeline_z", 
        "point_of_contact": "name", 
        "files": {
            "type": "csv", 
            "delimiter": ",", 
            "location": "s3a://cfa-dqb-dev/payload_staging/cfa_transfer_details_20220517.csv", 
            "header": true, 
            "multiple": true,
            "partition_column": "location_num"
            }, 
        "schema": [
      {
        "column_name": "TRANSFER_ID",
        "value_type": "IntegerType",
        "required": true
      },
      {
        "column_name": "ITEM_CODE",
        "value_type": "IntegerType",
        "required": true
      },
      {
        "column_name": "ITEM_DESCRIPTION",
        "value_type": "StringType",
        "required": true
      },
      {
        "column_name": "QUANTITY",
        "value_type": "IntegerType",
        "required": true
      },
        {
        "column_name": "TRANSFER_COST",
        "value_type": "DoubleType",
        "required": true
      },
        {
        "column_name": "ITEM_GROUP",
        "value_type": "StringType",
        "required": true
      },
        {
        "column_name": "GL_ACCOUNT_TOKEN_ID",
        "value_type": "StringType",
        "required": false
      },
        {
        "column_name": "LAST_MODIFIED_USER_NODE",
        "value_type": "IntegerType",
        "required": false
      },
        {
        "column_name": "LAST_MODIFIED_USER_ID",
        "value_type": "IntegerType",
        "required": false
      },
        {
        "column_name": "LAST_MODIFIED_TIMESTAMP",
        "value_type": "StringType",
        "required": false
      },
        {
        "column_name": "CREATED_TIMESTAMP",
        "value_type": "StringType",
        "required": false
      },
        {
        "column_name": "PROCESS_FLAG",
        "value_type": "StringType",
        "required": false
      },
        {
        "column_name": "PROCESS_DATE",
        "value_type": "TimestampType",
        "required": false
      },  
      {
        "column_name": "FUSION_PROCESS_FLAG",
        "value_type": "StringType",
        "required": false
      },
        {
        "column_name": "FUSION_PROCESS_DATE",
        "value_type": "StringType",
        "required": false
      },
        {
        "column_name": "REASON_CODE",
        "value_type": "StringType",
        "required": false
      }
    ],
        "result": {
            "location": "s3://cfa-dqb-dev/results/", 
            "raw_view": true, 
            "custom_view": {
                "create": true,
                "type": "detailed"
                }, 
            "name": "name"
            }, 
        "expectation_location": "s3a://cfa-dqb-dev/SupplyChain/Transfer Details Suite/transfer_details_suite.json", 
        "compute_space": {"nodes": 2, "version": "3.0", "size": "G.2x", "service": "Glue"}, 
        "request_from": "airflow"}, 
        "workflow": {
            "status": "pending 2022-07-22T08:58:02.611942+00:00", 
            "token_generation": "success 2022-07-22T08:58:02.611946+00:00", 
            "source_path_validation": "success 2022-07-22T09:04:55.095011+00:00", 
            "output_path_validation": "success 2022-07-22T09:04:55.630065+00:00", 
            "retrieve_expectation": "success 2022-07-22T09:04:55.363859+00:00", 
            "dqb": ["pending 2022-07-22T08:58:02.611954+00:00"], 
            "write_results": "pending 2022-07-22T08:58:02.611956+00:00", 
            "reason": ""
            }, 
        "token": "bb34ed6a-f4be-4efd-838a-0730fc844c3a", 
        "assets_bucket_link": "s3a://cfa-demo-config/", 
        "log_group": "DQB", 
        "sqs": "https://sqs.us-east-1.amazonaws.com/348855411931/test_preprocessor_output.fifo", 
        "table": "DQBProcessTable"
        }'


