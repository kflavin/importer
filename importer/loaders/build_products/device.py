import logging
import sys, os, re
from collections import OrderedDict
import xml.etree.ElementTree as ET

from importer.loaders.base import BaseLoader, convert_date

logger = logging.getLogger(__name__)

class MedDeviceCompleteLoader(BaseLoader):
    """
    Load Med Device data
    """

    # def load_directory(self, query, table_name, infile, batch_size=1000, throttle_size=10_000, throttle_time=3):
    def load_xml_files(self, query, indir, table_name, batch_size=1000, throttle_size=10_000, throttle_time=3):
        files = []
        for f in os.listdir(indir):
            if f.endswith(".xml"):
                files.append(f)
            # break # load one file for now

        pattern = re.compile(".*Part(\d+)")

        # dirs_clean = [ d for d in dirs if d.endswith(".xml") ]
        files.sort(key = lambda key: int(pattern.match(key).groups()[0]))

        logger.info("Loading files...")
        logger.info(files)

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

                # # print(dir(device.findall("d:publicDeviceRecordKey", ns)).pop())
                # pdrk = device.findall("d:publicDeviceRecordKey", ns)[0].text
                # print(pdrk)

                # identifiers = []

                for elem in device:
                    if elem.tag.endswith("publicDeviceRecordKey"):
                        # pdrk = elem.text
                        # Key order is important, so use an ordered dict and set order
                        item = OrderedDict()
                        item['publicDeviceRecordKey'] = elem.text
                        item['deviceId'] = None
                        item['deviceIdType'] = None
                        item['deviceDescription'] = None
                        item['companyName'] = None
                        item['phone'] = None
                        item['phoneExtension'] = None
                        item['email'] = None
                        item['brandName'] = None
                        item['dunsNumber'] = None
                        item['deviceIdIssuingAgency'] = None
                        item['containsDINumber'] = None
                        item['pkgQuantity'] = None
                        item['pkgDiscontinueDate'] = None
                        item['pkgStatus'] = None
                        item['pkgType'] = None
                        item['rx'] = None
                        item['otc'] = None
                        item['eff_date'] = None
                        item['end_eff_date'] = None

                    if elem.tag.endswith("identifiers"):
                        #identifiers = elem.findall('./{http://www.fda.gov/cdrh/gudid}identifier/')
                        identifiers = elem.findall(f'{prefix}identifier/')

                        # ids = {}
                        
                        print(elem)
                        for i in identifiers:
                            #pdb.set_trace()
                            if i.tag.endswith("deviceId"):
                                item['deviceId'] = i.text
                            if i.tag.endswith("deviceIdType"):
                                item['deviceIdType'] = i.text
                            if i.tag.endswith("deviceIdIssuingAgency"):
                                item['deviceIdIssuingAgency'] = i.text
                            if i.tag.endswith("containsDINumber"):
                                item['containsDINumber'] = i.text
                            if i.tag.endswith("pkgDiscontinueDate"):
                                item['pkgDiscontinueDate'] = i.text
                            if i.tag.endswith("pkgStatus"):
                                item['pkgStatus'] = i.text
                            if i.tag.endswith("pkgQuantity"):
                                item['pkgQuantity'] = i.text
                            if i.tag.endswith("pkgType"):
                                item['pkgType'] = i.text


                    if elem.tag.endswith("contacts"):
                        for i in elem.findall(f'd:customerContact/', ns):
                            if i.tag.endswith("phone"):
                                item['phone'] = i.text
                            if i.tag.endswith("phoneExtension"):
                                item['phoneExtension'] = i.text
                            if i.tag.endswith("email"):
                                item['email'] = i.text
                            # print(contact)
                            # devices[pdrk]['phone'] = contact.findall(f'd:phone', ns)[0].text
                            # devices[pdrk]['phoneExtension'] = contact.findall(f'd:phoneExtension', ns)[0].text
                            # devices[pdrk]['email'] = contact.findall(f'd:email', ns)[0].text


                        # print("contacts...")
                        # print(elem)
                        # #identifiers = elem.findall('./{http://www.fda.gov/cdrh/gudid}identifier/')
                        # for i in elem.findall(f'd:customerContact/', ns):
                        #     print("here...")
                        #     print(i)

                    if elem.tag.endswith("deviceDescription"):
                        item['deviceDescription'] = elem.text
                    if elem.tag.endswith("companyName"):
                        item['companyName'] = elem.text
                    if elem.tag.endswith("brandName"):
                        item['brandName'] = elem.text
                    if elem.tag.endswith("dunsNumber"):
                        item['dunsNumber'] = elem.text
                    if elem.tag.endswith("rx"):
                        item['rx'] = elem.text
                    if elem.tag.endswith("otc"):
                        item['otc'] = elem.text

                # put into list format, because that's what row loader expects right now
                # l = []
                # for k,v in item.items():
                #     l.append(v)

                devices.append(item)

            super().row_loader(query, columns, devices, table_name, batch_size, throttle_size, throttle_time)

        # print(len(item.keys()))
        # from pprint import pprint
        # pprint(devices)
        # print(columns)
        

