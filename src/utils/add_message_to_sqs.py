import os
import boto3
import json
import uuid





REGION = 'us-east-1'
sqs = boto3.client("sqs", region_name=REGION)


def add_message_to_sqs_queue(token, SQS_QUEUE):
  sqs_token = str(uuid.uuid4())
  message_body = json.dumps({
    'token': token
  })
  sqs.send_message(
    QueueUrl=SQS_QUEUE, 
    MessageBody=message_body,
    MessageAttributes={
      'string': {
        'StringValue': message_body,
        'DataType': "String"
      }
    },
    MessageDeduplicationId=sqs_token,
    MessageGroupId=sqs_token
  )

if __name__ == '__main__':
    import uuid
    token = str(uuid.uuid4())
    add_message_to_sqs_queue(token)