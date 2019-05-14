from datetime import date
from urllib.request import urlopen
import logging
from zipfile import ZipFile
import io
import csv
import re
import boto3
import botocore
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import selenium as se
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

class ProductDownloader(object):
    def __init__(self, region=None):
        options = se.webdriver.ChromeOptions()
        options.add_argument("--incognito")
        #options.add_argument('--ignore-certificate-errors')
        options.add_argument('headless')
        self.driver = webdriver.Chrome(options=options)
        logger.debug("Chrome started")
        self.today = date.today()
        self.datestr = f"{self.today.year}-{self.today.month}-{self.today.day}"
        self.jstimeout = 10

        if not region:
            # change this to autodetect region based on EC2 instance
            self.region = "us-east-1"

    def quit(self):
        self.driver.quit()

    ####################
    # File Downloads
    ####################

    def _s3_bucket_key(self, prefix):
        return f"{prefix}/archive"

    def dl_drugbank(self, url, bucket, prefix):
        filename = f"drugbank_{self.datestr}.zip"
        return self.url_to_s3(self.drugbank_url(url), filename, bucket, self._s3_bucket_key(prefix))

    def dl_indications(self, url, bucket, prefix):
        filename = f"indications_{self.datestr}.csv"
        return self.url_to_s3(self.indications_url(url), filename, bucket, self._s3_bucket_key(prefix))
        
    def dl_cms(self, url, bucket, prefix):
        original_file = f"cms_original_{self.datestr}.zip"
        binaryData = urlopen(self.cms_url(url)).read()

        # Upload the original file to S3
        res1 = self.url_to_s3(io.BytesIO(binaryData), original_file, bucket, self._s3_bucket_key(prefix), latest=False)

        # Upload the cleaned file to s3
        cleanedBinaryData = self.clean_cms(binaryData)
        cleaned_file = f"cms_clean_{self.datestr}.csv"
        res2 = self.url_to_s3(cleanedBinaryData, cleaned_file, bucket, self._s3_bucket_key(prefix))

        return True if res1 and res2 else False

    def dl_gudid(self, url, bucket, prefix):
        filename = f"gudid_{self.datestr}.zip"
        return self.url_to_s3(self.gudid_url(url), filename, bucket, self._s3_bucket_key(prefix))

    def dl_ndc(self, url, bucket, prefix):
        filename = f"ndc_{self.datestr}.zip"
        return self.url_to_s3(self.ndc_url(url), filename, bucket, self._s3_bucket_key(prefix))

    def dl_orange(self, url, bucket, prefix):
        filename = f"orangebook_{self.datestr}.zip"

        # As of 5/2/2019, we can download from the URL directly and get the latest zip.
        # return self.url_to_s3(self.orange_url(url), filename, bucket, self._s3_bucket_key(prefix))
        return self.url_to_s3(url, filename, bucket, self._s3_bucket_key(prefix))

    def dl_marketing_codes(self, url, bucket, prefix):
        binaryData = urlopen(url).read()

        # Upload the cleaned file to s3
        cleanedBinaryData = self.clean_marketing_codes(binaryData)
        cleaned_file = f"marketing_clean_{self.datestr}.csv"
        res = self.url_to_s3(cleanedBinaryData, cleaned_file, bucket, self._s3_bucket_key(prefix))

        return True if res else False

    #####################################
    # Find and return the download URL's
    #####################################

    def gudid_url(self, url):
        logger.debug("Scraping GUDID")
        html = urlopen(url).read()
        soup = BeautifulSoup(html, 'html.parser')
        # links = soup.select("a[download^=gudid_full_release]")
        links = soup.select("a[download^=AccessGUDID_Delimited_Full_Release]")
        return links[0].get('href')

    def ndc_url(self, url):
        logger.debug("Scraping NDC")
        html = urlopen(url).read()
        soup = BeautifulSoup(html, 'html.parser')
        links = soup.select("a[href$=ndctext.zip]")
        return links[0].get('href')

    # # As of 5/2/2019, we no longer need to scrape this page.
    # def orange_url(self, url):
    #     logger.debug("Scraping Orange Book")
    #     html = urlopen(url).read()
    #     soup = BeautifulSoup(html, 'html.parser')
    #     links = soup.select("a[href$=.zip]")
    #     base_url = "/".join(url.split("/")[:3])
    #     return base_url + links[0].get('href')

    def drugbank_url(self, url):
        logger.debug("Scraping Drug Bank")
        self.close_all_other_tabs(self.driver.current_window_handle)
        self.driver.get(url)

        try:
            elems = WebDriverWait(self.driver, self.jstimeout).until(lambda driver: driver.find_elements_by_xpath("//div[@id='open-data']//a[substring(@href, string-length(@href) - string-length('all-drugbank-vocabulary') + 1) = 'all-drugbank-vocabulary']"))
        except TimeoutException:
            logger.error("Drugbank page failure.")
            return ""

        dl_url = elems[0].get_attribute('href')
        logger.debug(f"Drugbank URL: {dl_url}")
        return dl_url

    def indications_url(self, url):
        logger.debug("Scraping Indications")
        self.close_all_other_tabs(self.driver.current_window_handle)
        # self.driver.delete_all_cookies()
        self.driver.get(url)

        try:
            elems = WebDriverWait(self.driver, self.jstimeout).until(lambda driver: driver.find_elements_by_xpath("//a[contains(text(), 'Download')]"))
        except TimeoutException:
            logger.error("Indications page one failure.")
            return ""

        elems[0].click()

        try:
            wait = WebDriverWait(self.driver, self.jstimeout)
            elem = wait.until(element_attribute_populated((By.ID, 'downloadFull'), "href"))
            dl_url = elem.get_attribute('href')
        except TimeoutException:
            logger.error("Indications page two failure.")
            return ""

        logger.debug(f"Indications URL: {dl_url}")
        return dl_url

    def cms_url(self, url):
        logger.debug("Scraping CMS Services")
        self.close_all_other_tabs(self.driver.current_window_handle)
        self.driver.get(url)

        try:
            elems = WebDriverWait(self.driver, self.jstimeout).until(lambda driver: driver.find_elements_by_xpath("//div[@class='help-details']//a[substring(@href, string-length(@href) - string-length('DHS-Addendum.zip') + 1) = 'DHS-Addendum.zip']"))
        except TimeoutException:
            logger.error("CMS page one failure.")
            return ""

        elems[0].click()

        # The download opens in a new window, so switch to it.
        logger.debug(f"Window handles: {self.driver.window_handles}")
        self.driver.switch_to.window(self.driver.window_handles[-1])

        try:
            elems2 = WebDriverWait(self.driver, self.jstimeout).until(lambda driver: driver.find_elements_by_xpath("//input[@name='agree' and @type='hidden']/parent::form"))
        except TimeoutException:
            logger.error("CMD page two failure")
            return ""

        accept_agreement = "?agree=yes&next=Accept"
        dl_url = elems2[0].get_attribute('action') + accept_agreement
        logger.debug(f"CMS URL: {dl_url}")
        return dl_url

    def create_latest_file(self, bucket, prefix, filename, latest_prefix, latest_file_name):
        s3 = boto3.resource('s3')
        # s3.Object(bucket,'my_file_old').delete()
        logger.debug(f"Copy most recent file: {bucket}/{prefix}/{filename}")
        logger.debug(f"To latest file: {bucket}/{latest_prefix}/{latest_file_name}")

        s3.Object(bucket,f"{latest_prefix}/{latest_file_name}").copy_from(CopySource=f"{bucket}/{prefix}/{filename}")

    def url_to_s3(self, url, filename, bucket, prefix, latest=True):
        """
        Download the zip file to the appropriate S3 folder.
        """
        if not url:
            logger.info(f"No URL provided for {filename}.")
            return False

        # fileName = url.split("/")[-1]
        client = boto3.client('s3')

        key = f"{prefix}/{filename}"
        latest_filename = "latest." + filename.split(".")[-1]   # keep the file extension
        latest_prefix = "/".join(prefix.split("/")[:-1])        # remove the year

        # Open the URL or expect a file-like object that returns bytes from read()
        if isinstance(url, str):
            logger.debug("Using a string URL")
            fileStream = urlopen(url)
        else:
            # print("binary stream")
            logger.debug("Using an IO object")
            fileStream = url

        # If it's already in our bucket, skip it.
        if not self.exists(bucket, key):
            logger.info(f"Uploading {bucket}/{key}")
            try:
                client.upload_fileobj(fileStream, bucket, key)
            except Exception as e:
                logger.error(f"Error while uploading {filename}: {e}")
                return False
        else:
            logger.info(f"Skipping {bucket}/{key}, already exists.")
        
        # Update the latest file
        if latest:
            self.create_latest_file(bucket, prefix, filename, latest_prefix, latest_filename)

        # Not needed?
        # self.driver.delete_all_cookies()

        return True
    
    def exists(self, bucket, key):
        """
        Check if the object exists in s3
        """
        s3 = boto3.resource('s3')

        try:
            s3.Object(bucket, key).load()
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                return False
            else:
                logger.warning(f"Unknown error while checking for {bucket}/{key}")
                raise
        
        return True

    def close_all_other_tabs(self, window_handle):
        for wh in self.driver.window_handles:
            if wh != window_handle:
                self.driver.switch_to.window(wh)
                self.driver.close()

        self.driver.switch_to.window(window_handle)

    def clean_marketing_codes(self, html):
        """
        The marketing codes are in an HTML table, and need parsing.
        """
        soup = BeautifulSoup(html, 'html.parser')
        table = soup.find_all("table")
        trs = table[0].find_all('tr')

        codes = []
        for i,tr in enumerate(trs):
            if len(tr.contents) != 2 :
                raise Exception(f"Unexpected column in row {i+1} of marketing codes")

            # Skip headers
            if tr.contents[0].name == 'th':
                continue

            term, code = tr.contents
            codes.append((term.text, code.text))

        out = io.StringIO()
        writer = csv.writer(out)
        writer.writerow(['spl_acceptable_term', 'code'])
        for code in codes:
            writer.writerow(code)

        out.seek(0)
        # get string value and encode as utf-8
        buffer = io.BytesIO(out.getvalue().encode('utf-8'))
        return buffer

    def clean_cms(self, binaryData):
        """
        CMS file is not parseable and requires manual cleaning.
        """
        zipfile = ZipFile(io.BytesIO(binaryData))

        rows = []
        for filen in zipfile.namelist():
            if filen.endswith(".txt"):
                for line in zipfile.open(filen).readlines():
                    line2 = line.decode('latin1')

                    # Look for lines that start with a 5 character code
                    m = re.match(r"^(\w{5})\t+([^\t]+)([\t]+)\r\n", line2)
                    if m:
                        two = m.group(2)
                        # Remove quotes
                        r = two[1:-1] if (two[0] == two[-1]) and two.startswith(("'", '"')) else two
                        rows.append((m.group(1), r))

        out = io.StringIO()
        writer = csv.writer(out)
        writer.writerow(['cpt_code', 'service'])
        for row in rows:
            writer.writerow(row)

        out.seek(0)
        buffer = io.BytesIO(out.getvalue().encode('utf-8'))
        return buffer


class element_attribute_populated(object):
  """An expectation for checking that an element has a particular css class.

  locator - used to find the element
  returns the WebElement once it has the particular css class
  """
  def __init__(self, locator, attribute):
    self.locator = locator
    self.attribute = attribute

  def __call__(self, driver):
    element = driver.find_element(*self.locator)   # Finding the referenced element
    if element.get_attribute(self.attribute):
        return element
    else:
        return False