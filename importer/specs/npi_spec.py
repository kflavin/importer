from mamba import description, context, it, before, after
from expects import expect, equal
from expects import *
from click.testing import CliRunner
from importer.commands.npi.commands import npi_preprocess, full
from importer.loaders import NpiLoader
from unittest import mock

with description('NPI') as self:
    with before.each:
        self.runner = CliRunner()

    with context("Valid usage"):
        with it('cleans a file '):
            self.result = self.runner.invoke(npi_preprocess, [
                                          '-i', 'importer/specs/files/npidata_pfile_10k.csv',
                                          '-o', '/dev/null'])
            expect(self.result.exit_code).to(equal((0)))
            expect(self.result.output.rstrip()).to(equal(("/dev/null")))

        with it('does a full load'):
            with mock.patch('importer.loaders.npi.loader.connector') as connector:
                self.result = self.runner.invoke(full, [
                                             '-u', 'someurl'
                                            ])
                # print(self.result.exit_code)
                # print(self.result.output)
                # print(self.result.exception)

        with after.each:
            assert not self.result.exception

    with context("Unit tests"):
        with before.each:
            self.npi_loader = NpiLoader()

        with it('DB setups up a cursor'):
            with mock.patch('importer.loaders.npi.loader.connector') as connector:
                args = {
                    'user': 'user',
                    'password': 'password',
                    'host': 'host',
                    'database': 'database'
                }

                self.npi_loader.connect(**args)
                expect(connector.connect.called).to(equal(True))
                expect(self.npi_loader.cursor).to(not_(equal('')))






# (url_prefix, batch_size, table_name, import_table_name, period, workspace, limit, large_file, environment, initialize)