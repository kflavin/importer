import logging, sys, os, re, time
from collections import OrderedDict
import xml.etree.ElementTree as ET

from importer.loaders.base import BaseLoader, convert_date

logger = logging.getLogger(__name__)

class RefreshLoader(BaseLoader):

    def __init__(self, *args, **kwargs):
        # self.DELTA_Q = kwargs.pop("DELTA_Q")
        # self.RETRIEVE_LEFT_Q = kwargs.pop("RETRIEVE_LEFT_Q")
        # self.RETRIEVE_RIGHT_Q = kwargs.pop("RETRIEVE_RIGHT_Q")
        # self.DELETE_Q = kwargs.pop("DELETE_Q")
        # self.ARCHIVE_Q = kwargs.pop("ARCHIVE_Q")
        # self.INSERT_Q = kwargs.pop("INSERT_Q")
        # self.join_columns = kwargs.pop("join_columns")
        # self.compare_columns = kwargs.pop("compare_columns")
        # self.extra_lcols = kwargs.pop("extra_lcols")
        # self.extra_rcols = kwargs.pop("extra_rcols", [])    # if needed in the future
        # self.insert_new_columns = kwargs.pop("insert_new_columns")
        # self.xform_left = kwargs.pop("xform_left")
        # self.xform_right = kwargs.pop("xform_right")
        # self.left_table_name = kwargs.pop("left_table_name")
        # self.right_table_name = kwargs.pop("right_table_name")
        # self.right_table_name_archive = kwargs.pop("right_table_name_archive")

        super().__init__(*args, **kwargs)

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

    def load_xml_files(self, query, indir, table_name, batch_size=1000, throttle_size=10_000, throttle_time=3):
        files = self._load_xml_files(indir)

        columns = [
            # "id",
            # "publicdevicerecordkey",
            "primarydi",
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

            # Iterate over <device> tags
            for device in root:
                if device.tag.endswith("header"):
                    continue

                # Maintain one dict of device elements, and a list of identifer elements
                identifier_list = []
                device_item = {}

                # Iterate over subelements of <device> tag.  Each device can have multiple identifiers,
                # which correspond to multiple rows in the device table.
                firstElem = True
                for elem in device:
                    # if elem.tag.endswith("publicDeviceRecordKey"):
                    if firstElem:
                        firstElem = False
                        # Device items are top level tags not under identifiers
                        # device_item['publicDeviceRecordKey'] = elem.text
                        # device_item['deviceId'] = None
                        # device_item['deviceIdType'] = None
                        # device_item['primaryDi'] = None

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
                        setDi = True
                        primaryDi = ""
                        primaryDiCount = 0  # check if we see more than one primary device

                        # Loop over all identifiers
                        for identifier in identifiers:
                            id_item = {}

                            # Iterate over subitems of single identifier
                            for i in identifier:
                                if i.tag.endswith("deviceId"):

                                    # We have the maintain the primaryDi across multiple identifiers.  If this
                                    # is type "Primary", save it.
                                    if setDi and id_item.get('deviceIdType', '').lower() == "primary":
                                        primaryDi = i.text
                                        setDi = False
                                    id_item['deviceId'] = i.text
                                if i.tag.endswith("deviceIdType"):
                                    # Likewise, save the primaryDi, if this is the primary.
                                    if i.text.lower() == "primary":
                                        # Warn if we've seen more than one primary type for this device
                                        primaryDiCount += 1
                                        if primaryDiCount > 1:
                                            logger.warn(f"Saw two devices of type primary! file: {f}, deviceId: {primaryDi}")

                                        if setDi and id_item.get('deviceId'):
                                            primaryDi = id_item.get('deviceId')
                                            setDi = False
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

                        # Update identifiers for this device with primaryDi
                        for identifier in identifier_list:
                            identifier['primaryDi'] = primaryDi

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
                    prepared_device['primaryDi']             = i.get('primaryDi')
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

    