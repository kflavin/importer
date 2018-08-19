import os
import boto3

class Downloader(object):
    def __init__(self, url_prefix):
        """
        URL includes the bucket name and prefix, without filename, ie: s3://my_bucket/path/to/file/
        """
        _, path = url_prefix.split(":")
        path = path.lstrip("/").rstrip("/")
        path_items = path.split("/")

        self.bucket = path_items.pop(0)
        self.prefix = "/".join(path_items)
        self.s3 = boto3.client('s3')

    def download(self, file_name, output_dir):
        """
        Download file from s3 into given file_path directory

        file_name: myfile.zip
        output_dir: /tmp/
        """

        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        key = f"{self.prefix}/{file_name}"
        output_file = f"{output_dir.rstrip('/')}/{file_name}"
        try:
            self.s3.download_file(self.bucket, key, output_file)
            return output_file
        except Exception as e:
            print(f"Error downloading {key}: {e}")
            return None

