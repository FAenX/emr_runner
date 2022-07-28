import boto3 
c_client = boto3.client('cloudwatch')
# 


class CloudWatchMetrics:
    def __init__(self, name_space):
        self.name_space = name_space

    def cloud_watch_metrics(self, process, time_elapsed):
        response = c_client.put_metric_data(
        Namespace= self.name_space,
        MetricData=[
            {
                'MetricName': 'load time',
                'Dimensions': [
                    {
                        'Name': 'name',
                        'Value': process
                    },
                ],
                'Value': time_elapsed,    
                'Unit': 'Seconds',
                
            },
        ])
        return response


if __name__ == '__main__':
    metrics = CloudWatchMetrics('cfa-demo')
    metrics.cloud_watch_metrics('process_a', 15)
