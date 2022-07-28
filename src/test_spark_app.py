from pyspark.sql import SparkSession
import argparse
import sys
import json
from utils.schema import Schema


def main():
    spark = SparkSession.builder.appName('CFA').getOrCreate()
    df = spark.read.format('csv').option("header", "true").load('data/total-usage-forecast-baseline')
    df.show()
    

if __name__ == '__main__':
    parser = argparse.ArgumentParser()    
    parser.add_argument(
        '--config', help="config.yaml location")   
    args = parser.parse_args()
    if not args.config:
        sys.exit('arguments not provided')
    config = args.config
    
    data = json.loads(config)

    schema = data.get('pipeline_config').get('schema')
    schema = Schema(schema)
    a = schema.get_schema()    

    print(a)
    # main()