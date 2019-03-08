import logging, csv, sys, subprocess, time, itertools, textwrap, datetime, re
from collections import OrderedDict
from zipfile import ZipFile
import mysql.connector as connector
from mysql.connector.constants import ClientFlag

from importer.sql.product import (INSERT_QUERY)
# from importer.sql.checks import DISABLE, ENABLE
# from importer.downloaders.downloader import downloader
from importer.loaders.base import BaseLoader, convert_date_time

import pandas as pd

logger = logging.getLogger(__name__)

class ProductLoader(BaseLoader):
    """
    Load NPI data.  There are two loaders in this file.abs

    File loader: Loads data in batches using an INSERT query.
    Large file loader: Loads data using LOAD DATA LOCAL INFILE query.
    """

    def __init__(self):
        super(ProductLoader, self).__init__()

    def merge(self, product_file, ndc_file):
        product = pd.read_csv(product_file)
        ndc = pd.read_csv(ndc_file)



    def preprocess(self, infile, outfile=None, encoding="ISO-8859-1"):
        """
        This takes an improperly formatted txt and turns it into a proper CSV :/
        """

        if not outfile:
            outfile = infile[:infile.rindex(".")] + ".clean.csv"

        tmpfile = infile[:infile.rindex(".")] + ".tmp.csv"

        with open(infile, "r", encoding="latin1") as f:
            csvfile = open(tmpfile, 'w', newline='\n')
            csvwriter = csv.writer(csvfile)

            for line in f:
                split = line.split(",")
                master_id = split.pop(0).strip()
                master_type = split.pop(0).strip()

                end_eff_date = split.pop().strip()
                eff_date = split.pop().strip()

                # if len(split) > 2:
                #     count += 1

                #     middle_fields = ",".join(split)

                #     # print(middle_fields)
                #     left = middle_fields.split(",")[0]
                #     right = middle_fields.split(",")[1:]
                   
                left = split[0]
                right = ",".join(split[1:])

                csvwriter.writerow([master_id, master_type, left, right, eff_date, end_eff_date])
                    # print(left)

                    # s = longestRepetitiveSubstring(middle_fields)
                    # if s:
                    #     if middle_fields[len(s)].startswith(s):
                    #         print(middle_fields)
                    #         print(s)

                        # be, le, ri = middle_fields.partition(s)
                        # csvwriter.writerow([master_id, master_type, s, ri, eff_date, end_eff_date])
                    # print("line: " + line)

                    # longest_lens.append(s)
                    
                    # csvwriter.writerow([master_id, master_type, *middle_fields, eff_date, end_eff_date])


                    # matches = re.findall(r'(.+)(?=.*\1)', middle_fields)
                    # largest = '' if not matches else max(matches,key=lambda m:len(m))
                    # delim = largest.count(",")
                    # one = ",".join(middle_fields.split(",")[:delim+1]).strip()
                    # two = ",".join(middle_fields.split(",")[delim+1:]).strip()
                    # print(one == two)
                    # print(f"{len(one)} {len(two)} {one} {two}")
                    # print(f"{len(left)} {len(right)} {left} {right}")

                # print(f"{master_id} {master_type} {eff_date} {end_eff_date}")
                
                # try:
                #     # newline = line.encode("ISO-8859-1").decode("utf-8")
                #     newline = line.encode("latin1").decode("utf-8")
                # except UnicodeDecodeError as e:
                #     print(count)
                #     print(line)
                #     error_count += 1
                #     print(error_count)
                #     # print(newline)
                

                # print("line")
                # print(line.encode("ISO-8859-1").decode("utf-8"))
                # if count == 48461:
                #     break
                # print("next")
            # print("Wrote: " + outfile)
            # print(csv.list_dialects())
            # print(longest_lens)
            # print(min(longest_lens))
        df = pd.read_csv(tmpfile)
        df.columns = [ super(ProductLoader, self)._clean_field(col) for col in df.columns]
        df['eff_date'] = df['eff_date'].apply(convert_date_time)
        df['end_eff_date'] = df['end_eff_date'].apply(convert_date_time)
        df.to_csv(outfile, sep=',', quoting=1, index=False)


    def load_file(self, table_name, infile, batch_size=1000, throttle_size=10_000, throttle_time=3):
        """
        Load NPI data using INSERT and UPDATE statements.  If a record in the data has been deactivated, then do an
        UPDATE instead of an INSERT to preserve the NPI data associated with the record.  This assumes the NPI record
        exists.  If it does not, the deactivated row will not be added.

        Optionally specify a batch and throttle sizes.  Batch size controls the number of rows sent to the DB at one time.  The
        throttling will sleep throttle_time seconds for every throttle_size rows.  Throttle_size should be >= to batch_size.  If
        either throttle arg is set to 0, throttling will be disabled.
        """
        logger.info("HDM loader importing from {}, batch size = {} throttle size={} throttle time={}"\
                .format(infile, batch_size, throttle_size, throttle_time))
        reader = csv.DictReader(open(infile, 'r'))
        insert_q = super().build_insert_query(INSERT_QUERY, super()._clean_fields(reader.fieldnames), table_name)
        # columnNames = reader.fieldnames

        row_count = 0
        batch = []
        batch_count = 1

        total_rows_modified = 0
        throttle_count = 0

        i = 0
        for row in reader:
            if row_count >= batch_size - 1:
                print("Submitting INSERT batch {}".format(batch_count))
                total_rows_modified += super()._submit_batch(insert_q, batch)
                batch = []
                row_count = 0
                batch_count += 1
            else:
                row_count += 1

            data = OrderedDict((super(ProductLoader, self)._clean_field(key), value) for key, value in row.items())
            batch.append(data)

            # Put in a sleep timer to throttle how hard we hit the database
            if throttle_time and throttle_size and (throttle_count >= throttle_size - 1):
                print(f"Sleeping for {throttle_time} seconds... row: {i}")
                time.sleep(int(throttle_time))
                throttle_count = 0
            elif throttle_time and throttle_size:
                throttle_count += 1
            i += 1

        # Submit remaining INSERT queries
        if batch:
            print("Submitting INSERT batch {}".format(batch_count))
            total_rows_modified += super()._submit_batch(insert_q, batch)

        return total_rows_modified


from collections import defaultdict
def getsubs(loc, s):
    substr = s[loc:]
    i = -1
    while(substr):
        yield substr
        substr = s[loc:i]
        i -= 1
def longestRepetitiveSubstring(r):
    occ = defaultdict(int)
    # tally all occurrences of all substrings
    for i in range(len(r)):
        for sub in getsubs(i,r):
            if r.startswith(sub):
                occ[sub] += 1
    # filter out all sub strings with fewer than 2 occurrences
    filtered = [k for k,v in occ.items() if v >= 2]
    if filtered:
        maxkey =  max(filtered, key=len) # Find longest string
        return maxkey
    else:
        return ""
        # raise ValueError("no repetitions of any substring of '%s' with 2 or more occurrences" % (r))

