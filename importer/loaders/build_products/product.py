import logging
from importer.loaders.base import BaseLoader, convert_date
import pandas as pd

logger = logging.getLogger(__name__)

class ProductLoader(BaseLoader):
    """
    Load Product Data
    """

    # def __init__(self):
    #     super().__init__()

    # def preprocess(self, infile, outfile=None, encoding="ISO-8859-1"):
    def preprocess(self, infile, outfile=None, encoding="latin1", excel=False):
        """
        Preprocess the product files
        """

        if not outfile:
            outfile = infile[:infile.rindex(".")] + ".clean.csv"

        if excel or infile.endswith(".xls") or infile.endswith(".xlsx"):
            df = pd.ExcelFile(infile).parse()
        else:
            df = pd.read_csv(infile, encoding=encoding)


        df.drop(list(df.filter(regex = '^Unnamed: ')), axis = 1, inplace = True)
        df.columns = [ super(ProductLoader, self)._clean_field(col) for col in df.columns]
        for col in df.filter(regex=".*_date"):
            df[col] = df[col].apply(convert_date)
        # df['eff_date'] = df['eff_date'].apply(convert_date)
        # df['end_eff_date'] = df['end_eff_date'].apply(convert_date)
        df.to_csv(outfile, sep=',', quoting=1, index=False)
