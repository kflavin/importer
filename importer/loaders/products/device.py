import logging, csv, sys, subprocess, time, itertools, textwrap, datetime, re
from collections import OrderedDict
from zipfile import ZipFile
import mysql.connector as connector
from mysql.connector.constants import ClientFlag

# from importer.sql.products.product import (INSERT_QUERY)
# from importer.sql.checks import DISABLE, ENABLE
# from importer.downloaders.downloader import downloader
from importer.loaders.base import BaseLoader, convert_date

import pandas as pd

logger = logging.getLogger(__name__)

class DeviceLoader(BaseLoader):

    def __init__(self):
        # super(DeviceLoader, self).__init__()
        super().__init__()

    # def preprocess(self, infile, outfile=None, encoding="ISO-8859-1"):
    def preprocess(self, infile, outfile=None, encoding="latin1"):
        if not outfile:
            outfile = infile[:infile.rindex(".")] + ".clean.csv"

        df = pd.read_csv(infile, encoding=encoding)
        df.columns = [ super(DeviceLoader, self)._clean_field(col) for col in df.columns]
        df['pkgdiscontinuedate'] = df['pkgdiscontinuedate'].apply(convert_date)
        df['eff_date'] = df['eff_date'].apply(convert_date)
        df['end_eff_date'] = df['end_eff_date'].apply(convert_date)
        df.to_csv(outfile, sep=',', quoting=1, index=False)

