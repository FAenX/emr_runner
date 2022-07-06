import json
import sys
import argparse
import yaml
import findspark
import boto3
import time
from pyspark.sql import SparkSession

from datetime import datetime
from utils import CloudWatchLogger
from utils import CloudWatchMetrics
from utils import DQBGe
from utils import read_yaml


client = boto3.client('s3')
s3 = boto3.resource('s3')
s3_client = boto3.client('s3')  
LOG_GROUP_NAME = 'DQB'
PROCESS_NAME = 'EMR'
logger = CloudWatchLogger(LOG_GROUP_NAME, PROCESS_NAME)
metrics = CloudWatchMetrics('cfa-demo')



def app(config):
    try: 
        START_TIME = time.time()
        logger.cloud_watch_logger({"event": 'DQB', "event_type": 'reading_config', 'process': 'EMR'}) 
        config = json.loads(config)       
        data = read_yaml(config)        
        # define schema        
        # print(read_data)
        source_bucket = data.get("source_bucket")
        source_key = data.get("source_key")
        header = data.get('header')
        expectations_bucket = data.get('expectations_bucket')
        expectations_prefix = data.get('expectations_prefix')
        expectations_name = data.get('expectations_name')       

        # load dataset
        DATA_SET_LOAD_START_TIME = time.time()
        logger.cloud_watch_logger({
            "event": 'DQB', 
            "event_type": 'loading_dataset',
            "process": "EMR"
            })  

    #     # end check config for multiple files and read files
    #     # finished loading dataset
        metrics.cloud_watch_metrics(
            'dataset_load_time',
            time.time() - DATA_SET_LOAD_START_TIME
        )     

        ge = DQBGe(
            expectations_bucket = expectations_bucket,
            expectations_prefix = expectations_prefix,
            data_docs_bucket = "cfa-demo-config",
            expectations_name = expectations_name,
            result_format = "BASIC",
            data_asset_name = expectations_name,
            batch_identifiers = expectations_name,
            include_unexpected_rows= False
        )

       
        spark = SparkSession.builder.appName('CFA').getOrCreate()
        schema = data.get('schema')

        try:
            file_location = 's3a://{0}/{1}'.format(source_bucket, source_key)
            logger.cloud_watch_logger({"event": 'DQB', "event_type": 'reading_single_file', 'process': 'EMR', "message": "file_location {}".format(file_location)})            
            df = spark.read.format('csv').option("header", header).schema(schema).load(file_location)            
            df.write.parquet("s3a://cfa-demo-config/parquets/" + file_location.split('/')[-1] + ".parquet") 
            compressed_df = spark.read.parquet("s3a://cfa-demo-config/parquets/" + file_location.split('/')[-1] + ".parquet")      
        except Exception as e:
            raise e


        print(ge.batch_identifiers)
        print(ge.check_point_name)
        datasource_config = ge.create_data_source()   
        ge.context.add_datasource(**datasource_config)         
        checkpoint_config = ge.create_check_point()
        # #      #add runtime checkpoint
        ge.context.add_checkpoint(**checkpoint_config)
        batch_request = ge.create_batch_request(compressed_df)        
        check_point_name, = ge.check_point_name
        results = ge.context.run_checkpoint(
            checkpoint_name=check_point_name,
            validations=[        
                {"batch_request": batch_request}    
            ],
            
        )

        print(results)
        GE_TEST_START_TIME = time.time() 
        logger.cloud_watch_logger({
            "event": 'DQB', 
            "event_type": 'completed_checkpoint',
            "message": "completed",
            "process": "EMR"            
        })

        metrics.cloud_watch_metrics(
            'checkpoint_load_time',
            time.time()-GE_TEST_START_TIME
        )        
        file_location = 's3a://{0}/{1}'.format(source_bucket, source_key)        
        # move files will also move out
        bucket = file_location.split('/',3)[2]
        key = file_location.split('/',3)[-1]
        destination_key = 'processed/' + key.split('/')[-1] 
        logger.cloud_watch_logger({
            "event": 'DQB', 
            "event_type": '',
            "message": "'moving_files",
            "process": "EMR"
            })

        MOVE_FILES_START_TIME = time.time()
        copy_source = {
            'Bucket': bucket, 
            'Key': key  
        }

        s3.meta.client.copy(
            copy_source, 
            bucket,
            destination_key
        )

        # delete file from staging
        s3.Object(
            bucket, 
            key
            ).delete()    

        expectation_results = results.to_json_dict()
        
        # write results
        client.put_object(
            Body=json.dumps(expectation_results), 
            Bucket='cfa-demo-pipeline2', 
            Key= 'processed/' + key.split('/')[-1].split('.')[0]+'_GE_RESULTS.json'
            )

        logger.cloud_watch_logger({
            "event": 'DQB', 
            "event_type": 'files_moved',
            "message": '',
            "process": "EMR"
            })
            
        metrics.cloud_watch_metrics(
            'copy_files',
            time.time()-MOVE_FILES_START_TIME
        )

         # clean up parquet folder
        parts = client.list_objects_v2(Bucket='cfa-demo-config', Prefix='parquets/' + file_location.split('/')[-1] + '.parquet')
        for i in parts['Contents']:
            s3.Object(
                'cfa-demo-config', 
                i['Key']
                ).delete()
    #     return    
    except Exception as e:
        message = {
            "event": "DQG", 
            "process": "EMR",
            "event_type": "error", 
            "error": str(e)
            
            }
        logger.cloud_watch_logger(message)       
        raise e

   
       

if __name__ == "__main__":
    parser = argparse.ArgumentParser()    
    parser.add_argument(
        '--config', help="config.yaml location")   
    args = parser.parse_args()
    if not args.config:
        sys.exit('arguments not provided')

    app(args.config)


    # j-1VR6PHHWH5F8H