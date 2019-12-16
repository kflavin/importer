from mamba import description, context, it, before, after
from expects import *
from click.testing import CliRunner
from importer.commands.products.download import (all, drugbank_url, indications_url, cms_url, gudid_url,
                    ndc_url, orange_url, marketing_codes_url, rxnorm_url)
from unittest import mock
from importer.downloaders.products.downloader import ProductDownloader

with description('Products') as self:
    with before.each:
        self.runner = CliRunner()

    with context("runs a full download"):
        with it('runs a full download'):
            with mock.patch('importer.downloaders.products.downloader.webdriver') as webdriver:
                with mock.patch('importer.downloaders.products.downloader.urlopen') as urlopen:
                    self.result = self.runner.invoke(all, ['-u', 'asdf'])

                    urlopen.assert_has_calls([
                        mock.call(rxnorm_url),
                        mock.call(gudid_url),
                        mock.call(marketing_codes_url),
                        mock.call(ndc_url),
                        # mock.call(orange_url),
                    ], any_order=True)

                    expect(urlopen.call_count).to(equal(6))

