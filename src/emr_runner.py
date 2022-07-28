from datetime import datetime
from datetime import timezone
import sys
import json
import boto3
from utils.ge import DQBGe
from utils.cloud_watch_logger import CloudWatchLogger
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from dao.dynamodb import DynamoDB
from utils.add_message_to_sqs import add_message_to_sqs_queue
from utils.schema import Schema
import argparse
 
client = boto3.client('s3')
s3 = boto3.resource('s3')
s3_client = boto3.client('s3')

class GlueRunner:
    def __init__(self, job_args, job_name):
        self.job_args = job_args
        self.job_name = job_name

    def run(self):
        pipeline_config = self.job_args.get('pipeline_config')
        schema = pipeline_config.get('schema')
        source_file_location = pipeline_config.get("files").get("location")
        header = pipeline_config.get('files').get('header')
        dynamodb_id = self.job_args.get('token')
        assets_bucket_link = self.job_args.get('assets_bucket_link')
        

        spark = SparkSession.builder.appName('CFA').getOrCreate()

        if schema is not None:
            schema = Schema(schema).get_schema()
            df = spark.read.format('csv').option("header", header).schema(schema).load(source_file_location)  
        else:
            df = spark.read.format('csv').option("header", header).load(source_file_location)          

        # print(read_data)
        pipeline_config=self.job_args.get('pipeline_config')        
        expectations_location = pipeline_config.get('expectation_location')
        split_expectation_location = expectations_location.split('//')[1].split('/', 1)
        expectations_bucket = split_expectation_location[0]
        expectations_prefix = split_expectation_location[1]
        expectations_name = expectations_prefix.split('/')[-1].split('.')[0]

        if expectations_prefix.split('/').__len__() > 1:
            parts = expectations_prefix.split('/')
            parts.pop()
            expectations_prefix = '/'.join(parts) + '/'

        result_format = pipeline_config.get('result').get('custom_view').get('type')
        print(result_format)
        if result_format.upper() == 'DETAILED':
            result_format = 'COMPLETE'

        split_assets_link = assets_bucket_link.split('//')[1].split('/', 1)
        assets_bucket = split_assets_link[0]

        ge_configuration = {
            'expectations_bucket': expectations_bucket, 
            'expectations_prefix': expectations_prefix, 
            'assets_bucket': assets_bucket, 
            'result_format': result_format
            }

        ge_instance = DQBGe(
           ge_configuration,
        )

        datasource_name = 's3_data_source'
        checkpoint_name = 's3_checkpoint'
        datasource_config = ge_instance.create_data_source(datasource_name)
        ge_instance.context.add_datasource(**datasource_config)         
        checkpoint_config = ge_instance.create_check_point(checkpoint_name, expectations_name)
        # #      #add runtime checkpoint


        ge_instance.context.add_checkpoint(**checkpoint_config)
        batch_request = ge_instance.create_batch_request(df, dynamodb_id, dynamodb_id, datasource_name)        
        results = ge_instance.context.run_checkpoint(
            checkpoint_name=checkpoint_name,
            validations=[        
                {"batch_request": batch_request}    
            ],
        )

        
        output_key = 'output/'
        formated_output_key = output_key +  token + '.json'        
        print(assets_bucket, output_key)
        expectation_results = results.to_json_dict() 
        # write results
        client.put_object(
            Body=json.dumps(expectation_results), 
            Bucket=assets_bucket, 
            Key= formated_output_key
            )

if __name__ == '__main__':
  
    parser = argparse.ArgumentParser()    
    parser.add_argument(
        '--config', help="config.yaml location"
        '--JOB_NAME', help="job name"
        )   
    args = parser.parse_args()
   
    job_name = args.JOB_NAME
    config = args.config
    
    data = json.loads(config)
    sqs = data.get('sqs')
    table = data.get('table')
    log_group = data.get('log_group') 

    logger = CloudWatchLogger(log_group_name=log_group, process_name=job_name)
    logger.cloud_watch_logger({'message': 'Started glue_runner.py'})

    
    glue_runner = GlueRunner(job_args=data, job_name=job_name) 
    
    
    workflow = data.get('workflow')
    token = data.get('token')
    dqb = workflow.get('dqb')
    pipeline_config = data.get('pipeline_config')
    assets_bucket_link = data.get('assets_bucket_link')

    dynamo_db = DynamoDB(table)

    dqb.append(f'started {datetime.now(timezone.utc).isoformat()}')
    dynamo_db.update_pipeline_workflow(workflow, token)
  
    try:  
        glue_runner.run()
        add_message_to_sqs_queue(token, sqs)
        dqb.append(f'success {datetime.now(timezone.utc).isoformat()}')
        workflow.update({'dqb': dqb})
        dynamo_db.update_pipeline_workflow(workflow, token)

        pipeline_config.update({'assets_bucket_link': assets_bucket_link})
        dynamo_db.update_pipeline_config(pipeline_config, token)
        logger.cloud_watch_logger({'message': 'Finished glue_runner.py'})
        
       
    except Exception as e: 
        workflow.update({
            "status": f"failed {datetime.now(timezone.utc).isoformat()}",
            "reason": str(e)
            })
        dqb.append(f'failed {datetime.now(timezone.utc).isoformat()}')
        workflow.update({'dqb': dqb})
        dynamo_db.update_pipeline_workflow(workflow, token)
        logger.cloud_watch_logger({'message': 'Failed glue_runner.py', 'error': str(e)})
        raise e
