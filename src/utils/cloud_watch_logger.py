import boto3
import time
import random
import json
l_client = boto3.client('logs')


class CloudWatchLogger:
    def __init__(self, log_group_name, process_name):
        self.seq_token = None
        self.log_group_name = log_group_name
        self.process_name = process_name
        self.log_stream_name = self._generate_log_stream_name()

    
    def _generate_log_stream_name(self):   
        return time.strftime("%m-%d-%Y %H-%M-%S", time.gmtime()) + \
        '-' + self.process_name

    def cloud_watch_logger(self, message_obj):
        '''
        cloud watch logger
        '''
        
        try:
            l_client.create_log_stream(
                logGroupName= self.log_group_name,
                logStreamName= self.log_stream_name
            )
        except Exception as e: 
            pass

        log_event = {
            "logGroupName" : self.log_group_name,
            "logStreamName" : self.log_stream_name,
            "logEvents":[            {
                    'timestamp': int(time.time()*1000),
                    'message': json.dumps(message_obj),                
                },
            ],
            
        }

        if self.seq_token is not None:
            log_event['sequenceToken'] = self.seq_token

        response = l_client.put_log_events(**log_event)
        if response.get('nextSequenceToken'):
            self.seq_token = response['nextSequenceToken']


if __name__ == '__main__':
    LOG_GROUP_NAME = '/aws/lambda/cfa-demo-function'
    logger = CloudWatchLogger(LOG_GROUP_NAME, 'DQB')

    logger.cloud_watch_logger({'event': 'test', 'event_type': 'test_event_type'})
    print(logger.seq_token)
    logger.cloud_watch_logger({'event': 'test', 'event_type': 'test_event_type'})
    