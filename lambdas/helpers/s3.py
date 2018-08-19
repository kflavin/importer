import boto3
from pprint import pprint

class S3Helper(object):
    def __init__(self):
        self.client = boto3.client('s3')

    def get_last_modified(self, obj):
        return obj['LastModified']

    def next_bucket_key(self, bucket, prefix):
        """
        Return the key of the most recently downloaded object, given a bucket and prefix
        """
        objects = self.client.list_objects_v2(Bucket=bucket, Prefix=prefix)

        most_recent = None
        if "Contents" in objects:
            most_recent = sorted(objects['Contents'], key=get_last_modified).pop()['Key'] # last object

        return most_recent

    def is_imported(self, bucket, key):
        """
        Check if a file has been imported 
        """
        obj = self.client.get_object_tagging(Bucket=bucket, Key=key)

        for tags in obj.get('TagSet'):
            if tags['Key'] == "imported":
                if tags['Value'] == "true":
                    return True

        return False

    def find_unimported(self, bucket, prefix, max=4):
        """
        Find up to max files under, bucket and prefix, that were not imported, sorted by modification date.  
        Note that due to S3 limitations, we cannot filter by tag.
        """
        print("Finding unimported objects")
        objects = self.client.list_objects_v2(Bucket=bucket, Prefix=prefix)

        most_recent = []
        if "Contents" in objects:
            for idx,obj in enumerate(sorted(objects['Contents'], key=get_last_modified, reverse=True)):
                if idx >= max:
                    break
                most_recent.append(obj['Key'])

            # print(most_recent)

            imported_status = { m: is_imported(bucket, m) for m in most_recent }
            not_imported = { k: v for k,v in imported_status.items() if not v }
            
            return not_imported


if __name__ == "__main__":
    # Test from CLI
    import sys
    # print(next_bucket_key(sys.argv[1], sys.argv[2]))
    # pprint(is_imported(sys.argv[1], sys.argv[2]))
    # pprint(find_unimported(sys.argv[1], sys.argv[2]))
