import boto3
from botocore.exceptions import ClientError
import time
from pprint import pprint

class CloudWatchLogger(object):
    def __init__(self, group, stream, region):
        self.logger = boto3.client('logs', region_name=region)
        self.log_group = group
        self.log_stream = stream
        self.response = ""
        self.token = ""

        try: 
            self.logger.create_log_group(logGroupName=self.log_group)
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceAlreadyExistsException':
                print("Group already exists, skip creating.")

        try:
            self.logger.create_log_stream(logGroupName=self.log_group, logStreamName=self.log_stream)
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceAlreadyExistsException':
                print("Steam already exists, skip creating.")

        response = self.logger.describe_log_streams(
            logGroupName=self.log_group,
            logStreamNamePrefix=self.log_stream,
            # orderBy='LogStreamName'|'LastEventTime',
            # orderBy='LastEventTime',
            # descending=True|False,
            # descending=True,
            # nextToken='string',
            limit=1
        )
        print(response)

        # if response and "uploadSequenceToken" in response.get('logStreams')[0]:
        if response and "logStreams" in response:
            if len(response.get('logStreams')) > 0:
                if "uploadSequenceToken" in response['logStreams'][0]:
                    print("We have a token!")
                    self.token = response['logStreams'][0]['uploadSequenceToken']

    def info(self, message):
        timestamp = int(round(time.time() * 1000))

        print(f"group name is {self.log_group}")

        args = {
            "logGroupName": self.log_group,
            "logStreamName": self.log_stream,
            "logEvents": [
                {
                    'timestamp': timestamp,
                    # 'message': f"{time.strftime('%Y-%m-%d %H:%M:%S')} {message}"
                    'message': message
                }
            ]
        }

        if self.token:
            args['sequenceToken'] = self.token

        self.response = self.logger.put_log_events(**args)

        if self.response:
            pprint(self.response)
            self.token = self.response['nextSequenceToken']



if __name__ == "__main__":
    logger = CloudWatchLogger("npi-importer", "mystream", "us-east-1")
    logger.info("One...")
    logger.info("Two....")