import boto3
import json
from datetime import datetime
from datetime import timezone




REGION = 'us-east-1'

dynamodb = boto3.resource('dynamodb', 'us-east-1')

class DynamoDB:
  def __init__(self, table_name):
    self.table_name = table_name
    self.table = dynamodb.Table(table_name)


  def update_pipeline_workflow(self, workflow, token):
    
    return self.table.update_item(
      
      Key = {
        'token': token
      },
      UpdateExpression = 'SET workflow = :val1, updated_date = :val2',
      ExpressionAttributeValues = {
                  ':val1': json.dumps(workflow),
                  ':val2': datetime.now(timezone.utc).isoformat()
              },
    )


  def update_pipeline_config(self, pipeline_config, token):
    return self.table.update_item(
      
      Key = {
        'token': token
      },
      UpdateExpression = 'SET pipeline_config = :val1, updated_date = :val2',
      ExpressionAttributeValues = {
                  ':val1': json.dumps(pipeline_config),
                  ':val2': datetime.now(timezone.utc).isoformat()
              },
    )





  