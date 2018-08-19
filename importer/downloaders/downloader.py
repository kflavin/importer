
def downloader(url_prefix):
    """
    Downloader factory
    """
    protocol, path = url_prefix.split(":")

    if protocol.lower() == "s3":
        from importer.downloaders.s3 import Downloader
        return Downloader(url_prefix)
    else:
        # in case we want to use something other than AWS
        pass



