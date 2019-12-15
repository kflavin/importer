import logging
from importer.loaders.base import BaseLoader, convert_date
from importer.sql import SELECT_Q

logger = logging.getLogger(__name__)

class GudidLoader(BaseLoader):


    def load_devices(self, query, stage_table_name, target_table_name, batch_size=1000, throttle_size=10_000, throttle_time=3):
        logger.info("Select data from staging table")
        select_q = SELECT_Q.format(table_name="stage_fda_device")
        cursor = self._query(select_q, returnCursor=True)
        logger.info("Data selected")
        columns = cursor.column_names
        rows = list(cursor)
        logger.info("Pull data into list")

        # print(dir(rows))

        # for row in rows:
            # print(f"row = {row}")

        logger.info("Start row loader")
        self.row_loader(query, columns, rows, target_table_name, batch_size=batch_size, throttle_size=throttle_size, throttle_time=throttle_time)
        logger.info("Finish row loader")

        

    # def preprocess(self, infile, outfile=None, encoding="latin1"):
    #     if not outfile:
    #         outfile = infile[:infile.rindex(".")] + ".clean.csv"

    #     if infile.endswith(".xls") or infile.endswith(".xlsx"):
    #         df = pd.ExcelFile(infile).parse()
    #     else:
    #         df = pd.read_csv(infile, encoding=encoding)

    #     df.columns = [ super(DeviceLoader, self)._clean_field(col) for col in df.columns]
    #     df['pkgdiscontinuedate'] = df['pkgdiscontinuedate'].apply(convert_date)
    #     df['eff_date'] = df['eff_date'].apply(convert_date)
    #     df['end_eff_date'] = df['end_eff_date'].apply(convert_date)
    #     df.to_csv(outfile, sep=',', quoting=1, index=False)

