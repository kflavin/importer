from datetime import date
from time import sleep
from urllib.request import urlopen
import logging
import boto3
import botocore
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
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
        self.today = date.today()
        self.datestr = f"{self.today.year}-{self.today.month}-{self.today.day}"
        self.jstimeout = 10

        if not region:
            # change this to autodetect region based on EC2 instance
            self.region = "us-east-1"

    ####################
    # File Downloads
    ####################

    def dl_drugbank(self, url, bucket, prefix):
        filename = f"drugbank_{self.datestr}.zip"
        return self.url_to_s3(self.drugbank_url(url), filename, bucket, f"{prefix}/{self.today.year}")

    def dl_indications(self, url, bucket, prefix):
        filename = f"indications_{self.datestr}.csv"
        return self.url_to_s3(self.indications_url(url), filename, bucket, f"{prefix}/{self.today.year}")
        
    def dl_cms(self, url, bucket, prefix):
        filename = f"cms_{self.datestr}.zip"
        return self.url_to_s3(self.cms_url(url), filename, bucket, f"{prefix}/{self.today.year}")

    def dl_gudid(self, url, bucket, prefix):
        filename = f"gudid_{self.datestr}.zip"
        return self.url_to_s3(self.gudid_url(url), filename, bucket, f"{prefix}/{self.today.year}")

    def dl_ndc(self, url, bucket, prefix):
        filename = f"ndc_{self.datestr}.zip"
        return self.url_to_s3(self.ndc_url(url), filename, bucket, f"{prefix}/{self.today.year}")

    def dl_orange(self, url, bucket, prefix):
        filename = f"orangebook_{self.datestr}.zip"
        return self.url_to_s3(self.orange_url(url), filename, bucket, f"{prefix}/{self.today.year}")

    ####################
    # URL's
    ####################

    def gudid_url(self, url):
        html = urlopen(url).read()
        soup = BeautifulSoup(html, 'html.parser')
        links = soup.select("a[download^=gudid_full_release]")
        return links[0].get('href')

    def ndc_url(self, url):
        html = urlopen(url).read()
        soup = BeautifulSoup(html, 'html.parser')
        links = soup.select("a[href$=ndctext.zip]")
        return links[0].get('href')

    def orange_url(self, url):
        html = urlopen(url).read()
        soup = BeautifulSoup(html, 'html.parser')
        links = soup.select("a[href$=.zip]")
        base_url = "/".join(url.split("/")[:3])
        return base_url + links[0].get('href')

    def drugbank_url(self, url):
        self.close_all_tabs(self.driver.current_window_handle)
        self.driver.get(url)
        sleep(1)
        el = self.driver.find_elements_by_xpath("//div[@id='open-data']//a[substring(@href, string-length(@href) - string-length('all-drugbank-vocabulary') + 1) = 'all-drugbank-vocabulary']")
        dl_url = el[0].get_attribute('href')
        logger.debug(f"Drugbank URL: {dl_url}")
        return dl_url

    def indications_url(self, url):
        self.close_all_tabs(self.driver.current_window_handle)
        self.driver.get(url)
        el = self.driver.find_elements_by_xpath("//a[contains(text(), 'Download')]")
        r = el[0].click()
        el2 = self.driver.find_elements_by_xpath("//a[@id='downloadFull']")
        r = el2[0].click()
        sleep(1)
        logger.debug(f"Indications URL: {el2[0].get_attribute('href')}")
        dl_url = el2[0].get_attribute('href')
        return dl_url

    def cms_url(self, url):
        self.close_all_tabs(self.driver.current_window_handle)
        self.driver.get(url)

        try:
            elems = WebDriverWait(self.driver, self.jstimeout).until(lambda driver: driver.find_elements_by_xpath("//div[@class='help-details']//a[substring(@href, string-length(@href) - string-length('DHS-Addendum.zip') + 1) = 'DHS-Addendum.zip']"))
            # do smth with the found element
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
        logger.debug(f"Latest: {bucket}/{latest_prefix}/{latest_file_name}")
        logger.debug(f"Most recent date: {bucket}/{prefix}/{filename}")

        s3.Object(bucket,f"{latest_prefix}/{latest_file_name}").copy_from(CopySource=f"{bucket}/{prefix}/{filename}")

    def url_to_s3(self, url, filename, bucket, prefix):
        """
        Download the zip file to the appropriate S3 folder.
        """
        if not url:
            logger.info(f"No URL provided for {filename}.")
            return False

        zippedFile = urlopen(url)
        # fileName = url.split("/")[-1]
        client = boto3.client('s3')

        key = f"{prefix}/{filename}"
        latest_filename = "latest." + filename.split(".")[-1]   # keep the file extension
        latest_prefix = "/".join(prefix.split("/")[:-1])        # remove the year

        # If it's already in our bucket, skip it.
        if not self.exists(bucket, key):
            logger.info(f"Uploading {bucket}/{key}")
            try:
                client.upload_fileobj(zippedFile, bucket, key)
            except Exception as e:
                logger.error(f"Error while uploading {filename}: {e}")
                return False
        else:
            logger.info(f"Skipping {bucket}/{key}, already exists.")
        
        # Update the latest file
        self.create_latest_file(bucket, prefix, filename, latest_prefix, latest_filename)
        self.driver.delete_all_cookies()

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
                logger.warning("Unknown error.")
                raise
        
        return True

    def close_all_tabs(self, window_handle):
        for wh in self.driver.window_handles:
            if wh != window_handle:
                self.driver.switch_to.window(wh)
                self.driver.close()

        self.driver.switch_to.window(window_handle)
