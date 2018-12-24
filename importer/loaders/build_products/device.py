import logging, sys, os, re, time
from collections import OrderedDict
import xml.etree.ElementTree as ET

from importer.loaders.base import BaseLoader, convert_date
from importer.sql import (DELETE_Q)
from importer.sql.products.device import (RETRIEVE_RECORDS_Q, INSERT_Q, DELTA_RECORDS_Q)


logger = logging.getLogger(__name__)

class MedDeviceCompleteLoader(BaseLoader):
    """
    Load Med Device data
    """

    def _load_xml_files(self, indir):
        files = []
        for f in os.listdir(indir):
            if f.endswith(".xml"):
                files.append(f)

        pattern = re.compile(".*Part(\d+)")

        # dirs_clean = [ d for d in dirs if d.endswith(".xml") ]
        try:
            files.sort(key = lambda key: int(pattern.match(key).groups()[0]))
        except Exception as e:
            logger.error("Bad filename pattern.")
            raise

        # files = [files[0]] # toggle to load one file only

        logger.info(f"Loading {len(files)} files...")
        logger.debug(files)
        return files

    def _delta_q(self, query):
        """
        Return a list of found rows and not found rows.
        """
        try:
            self.cursor.execute(query)
        except Exception as e:
            raise

        found = []
        not_found = []
        for f in self.cursor:
            if f[3] == 0:
                not_found.append(f[:3])
            elif f[3] == 1:
                found.append(f[:3])
            else:
                logger.warn(f"I don't recognize f[3]={f[3]}!  Skipping...")

        return found, not_found

    def build_retrieve_query(self, query, rows, table_name):
        where_clause = []

        for row in rows:
            """
            row[0]: pdrk, row[1]: deviceid, row[2]: deviceidtype
            """
            where_clause.append("(publicdevicerecordkey='{}' AND deviceid='{}' AND deviceidtype='{}')".format(row[0], row[1], row[2]))
        

        where_clause_s = " OR ".join(where_clause)
        # where_clause_s = where_clause_s[:-3]

        return query.format(table_name=table_name, where_clause=where_clause_s)

    def delta_stage_to_prod(self, stage_table_name, prod_table_name, batch_size=1000, throttle_size=10_000, throttle_time=3):
        """
        Run a delta between an old table and a new table.  Return two lists of ID's.
            First: New records
            Second: Existing records that require updates
        """
        logger.info(f"Performing delta update of {stage_table_name} and {prod_table_name}")
        # files = self._load_xml_files(indir)
        # print(f"prod table is: {prod_table_name}")
        # print(f"stage table is: {stage_table_name}")

        q = DELTA_RECORDS_Q.format(stage_table=stage_table_name, prod_table=prod_table_name)

        found, not_found = self._delta_q(q)
        # print(found)
        # print(not_found)

        total_update_count = 0
        if found:
            # These records need to be updated
            for c, batch in enumerate(super()._batcher(found, batch_size, throttle_size, throttle_time)):
                logger.info("Check batch for updates {}".format(c+1))
                # use this for product table updates
                q = self.build_retrieve_query(RETRIEVE_RECORDS_Q, batch, stage_table_name)
                stage = super()._submit_single_q(q)

                q = self.build_retrieve_query(RETRIEVE_RECORDS_Q, batch, prod_table_name)
                prod = super()._submit_single_q(q)

                need_updates = []
                for row in stage:
                    if row not in prod:
                        # ID exists, but the row has changed
                        need_updates.append(row)
                    else:
                        pass

                # print(prod_table_name)
                if need_updates:
                    logger.info(f"{len(need_updates)} rows need updates")
                    total_update_count += len(need_updates)
                    # Delete the row with changes
                    q = self.build_retrieve_query(DELETE_Q, need_updates, prod_table_name)
                    # print(q)
                    result = super()._submit_single_q(q)
                    # print(result)

                    # Insert the new row
                    # q = self.build_insert_query(INSERT_Q, need_updates, prod_table_name)
                    q = INSERT_Q.format(table_name=prod_table_name)
                    # print(q)
                    # print("need updates")
                    # print(need_updates)
                    result = super()._submit_batch(q, need_updates)
                    # print('result of need updates')
                    # print(result)

        logger.info(f"Updated {total_update_count} records")

        total_insert_count = 0
        if not_found:
            # These are new records that need to be inserted.
            for c, batch in enumerate(super()._batcher(not_found, batch_size, throttle_size, throttle_time)):
                logger.info(f"Submitting INSERT batch {c+1}")
                total_insert_count += len(batch)
                
                # Retrieve the full record we need from staging
                q = self.build_retrieve_query(RETRIEVE_RECORDS_Q, batch, stage_table_name)
                new_records = super()._submit_single_q(q)

                # print("new records")
                # print(new)
                
                if new_records:
                    q = INSERT_Q.format(table_name=prod_table_name)
                    result = super()._submit_batch(q, new_records)
                    # print("result of new insert")
                    # print(result)

        logger.info(f"Inserted {total_insert_count} records")

    # def load_directory(self, query, table_name, infile, batch_size=1000, throttle_size=10_000, throttle_time=3):
    def load_xml_files(self, query, indir, table_name, batch_size=1000, throttle_size=10_000, throttle_time=3):
        files = self._load_xml_files(indir)

        columns = [
            # "id",
            "publicdevicerecordkey",
            "deviceid",
            "deviceidtype",
            "devicedescription",
            "companyname",
            "phone",
            "phoneextension",
            "email",
            "brandname",
            "dunsnumber",
            "deviceidissuingagency",
            "containsdinumber",
            "pkgquantity",
            "pkgdiscontinuedate",
            "pkgstatus",
            "pkgtype",
            "rx",
            "otc",
            "eff_date",
            "end_eff_date"
            # "created_at",
            # "updated_at"
        ]

        ns = {'d': 'http://www.fda.gov/cdrh/gudid'}


        for f in files:
            logger.info(f)
            tree = ET.parse(f"{indir}/{f}")
            root = tree.getroot()

            prefix = '{http://www.fda.gov/cdrh/gudid}'
            count = 0
            # devices = {}
            devices = []
            for device in root:
                if device.tag.endswith("header"):
                    continue

                # Maintain one dict of device elements, and a list of identifer elements
                identifier_list = []
                device_item = {}

                for elem in device:
                    if elem.tag.endswith("publicDeviceRecordKey"):
                        device_item['publicDeviceRecordKey'] = elem.text
                        # device_item['deviceId'] = None
                        # device_item['deviceIdType'] = None
                        device_item['deviceDescription'] = None
                        device_item['companyName'] = None
                        device_item['phone'] = None
                        device_item['phoneExtension'] = None
                        device_item['email'] = None
                        device_item['brandName'] = None
                        device_item['dunsNumber'] = None
                        # device_item['deviceIdIssuingAgency'] = None
                        # device_item['containsDINumber'] = None
                        # device_item['pkgQuantity'] = None
                        # device_item['pkgDiscontinueDate'] = None
                        # device_item['pkgStatus'] = None
                        # device_item['pkgType'] = None
                        device_item['rx'] = None
                        device_item['otc'] = None
                        device_item['eff_date'] = None
                        device_item['end_eff_date'] = None

                    # Each device can have multiple identifers, which are separate rows in our table
                    if elem.tag.endswith("identifiers"):
                        # identifiers = elem.findall(f'{prefix}identifier/')
                        identifiers = elem.findall(f'{prefix}identifier')

                        # Loop over all identifiers
                        for identifier in identifiers:
                            id_item = {}
                            for i in identifier:
                                if i.tag.endswith("deviceId"):
                                    id_item['deviceId'] = i.text
                                if i.tag.endswith("deviceIdType"):
                                    id_item['deviceIdType'] = i.text
                                if i.tag.endswith("deviceIdIssuingAgency"):
                                    id_item['deviceIdIssuingAgency'] = i.text
                                if i.tag.endswith("containsDINumber"):
                                    id_item['containsDINumber'] = i.text
                                if i.tag.endswith("pkgDiscontinueDate"):
                                    id_item['pkgDiscontinueDate'] = i.text
                                if i.tag.endswith("pkgStatus"):
                                    id_item['pkgStatus'] = i.text
                                if i.tag.endswith("pkgQuantity"):
                                    id_item['pkgQuantity'] = i.text
                                if i.tag.endswith("pkgType"):
                                    id_item['pkgType'] = i.text

                            identifier_list.append(id_item)


                    if elem.tag.endswith("contacts"):
                        for i in elem.findall(f'd:customerContact/', ns):
                            if i.tag.endswith("phone"):
                                device_item['phone'] = i.text
                            if i.tag.endswith("phoneExtension"):
                                device_item['phoneExtension'] = i.text
                            if i.tag.endswith("email"):
                                device_item['email'] = i.text

                    if elem.tag.endswith("deviceDescription"):
                        device_item['deviceDescription'] = elem.text
                    if elem.tag.endswith("companyName"):
                        device_item['companyName'] = elem.text
                    if elem.tag.endswith("brandName"):
                        device_item['brandName'] = elem.text
                    if elem.tag.endswith("dunsNumber"):
                        device_item['dunsNumber'] = elem.text
                    if elem.tag.endswith("rx"):
                        device_item['rx'] = elem.text
                    if elem.tag.endswith("otc"):
                        device_item['otc'] = elem.text

                # For each identifier, create a separate dict item (db row) to load
                for i in identifier_list:
                    # Key order is important, so use OrderedDict
                    prepared_device = OrderedDict()
                    prepared_device['publicDeviceRecordKey'] = device_item.get('publicDeviceRecordKey')
                    prepared_device['deviceId']              = i.get('deviceId')
                    prepared_device['deviceIdType']          = i.get('deviceIdType')
                    prepared_device['deviceDescription']     = device_item.get('deviceDescription')
                    prepared_device['companyName']           = device_item.get('companyName')
                    prepared_device['phone']                 = device_item.get('phone')
                    prepared_device['phoneExtension']        = device_item.get('phoneExtension')
                    prepared_device['email']                 = device_item.get('email')
                    prepared_device['brandName']             = device_item.get('brandName')
                    prepared_device['dunsNumber']            = device_item.get('dunsNumber')
                    prepared_device['deviceIdIssuingAgency'] = i.get('deviceIdIssuingAgency')
                    prepared_device['containsDINumber']      = i.get('containsDINumber')
                    prepared_device['pkgQuantity']           = i.get('pkgQuantity')
                    prepared_device['pkgDiscontinueDate']    = i.get('pkgDiscontinueDate')
                    prepared_device['pkgStatus']             = i.get('pkgStatus')
                    prepared_device['pkgType']               = i.get('pkgType')
                    prepared_device['rx']                    = device_item.get('rx')
                    prepared_device['otc']                   = device_item.get('otc')
                    prepared_device['eff_date']              = device_item.get('eff_date')
                    prepared_device['end_eff_date']          = device_item.get('end_eff_date')

                    devices.append(prepared_device)

            super().row_loader(query, columns, devices, table_name, batch_size, throttle_size, throttle_time)

        

