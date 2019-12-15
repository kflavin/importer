

class NpiDownloader(object):

    @staticmethod
    def get_downloader(url_prefix):
        """
        Downloader factory
        """
        protocol, path = url_prefix.split(":")

        if protocol.lower() == "s3":
            from importer.downloaders import S3Downloader
            return S3Downloader(url_prefix)
        else:
            # in case we want to use something other than AWS
            pass



