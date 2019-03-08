import logging
from importer.loaders.base import BaseLoader, convert_date
import pandas as pd

logger = logging.getLogger(__name__)

# Deprecated, remove
# class NdcLoader(BaseLoader):
#     """
#     Load NDC Master data
#     """

#     # def __init__(self):
#     #     super().__init__()

#     def preprocess(self, infile, outfile=None):
#         """
#         Preprocess the NDC Master file.
#         """

#         if not outfile:
#             outfile = infile[:infile.rindex(".")] + ".clean.csv"

#         df = pd.ExcelFile(infile).parse()

#         df.columns = [ super(NdcLoader, self)._clean_field(col) for col in df.columns]
#         df['eff_date'] = df['eff_date'].apply(convert_date)
#         df['end_eff_date'] = df['end_eff_date'].apply(convert_date)
        
#         df.to_csv(outfile, sep=',', quoting=1, index=False, encoding='utf-8')

#         return outfile


