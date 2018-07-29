import boto3
from pprint import pprint

client = boto3.client('s3')

def get_last_modified(obj):
    return obj['LastModified']

def next_bucket_key(bucket, prefix):
    """
    Return the key of the most recently downloaded object, given a bucket and prefix
    """
    objects = client.list_objects_v2(Bucket=bucket, Prefix=prefix)

    most_recent = None
    if "Contents" in objects:
        most_recent = sorted(objects['Contents'], key=get_last_modified).pop()['Key']

    return most_recent

def is_imported(bucket, key):
    """
    Check if a file has been imported 
    """
    obj = client.get_object_tagging(Bucket=bucket, Key=key)

    for tags in obj.get('TagSet'):
        if tags['Key'] == "imported":
            if tags['Value'] == "true":
                return True

    return False

if __name__ == "__main__":
    # Test from CLI
    import sys
    # print(next_bucket_key(sys.argv[1], sys.argv[2]))
    pprint(is_imported(sys.argv[1], sys.argv[2]))
