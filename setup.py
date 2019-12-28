from setuptools import setup, find_packages
import distutils
import shutil
import glob
import os
 

class CleanCommand(distutils.cmd.Command):
    user_options = []
    CLEAN_FILES = './build ./dist ./*.egg-info'.split()

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        here = os.path.dirname(os.path.realpath(__file__))

        for path_spec in self.CLEAN_FILES:
            abs_paths = glob.glob(os.path.normpath(os.path.join(here, path_spec)))
            for path in [str(p) for p in abs_paths]:
                if not path.startswith(here):
                    raise ValueError(f"{path} is not a path inside {here}")
                print(f'removing {os.path.relpath(path)}')
                shutil.rmtree(path)


setup(name='importer',
      version='0.12.0',
      description='Importer',
      scripts=['./runner-import.py',],
      #packages=['importer.downloaders', 'importer.loaders', 'importer.sql', 'importer.loggers', 'importer.commands'],
      packages=find_packages(exclude=("lambda",)),
      # data_files=[('./', ['importer/requirements.npi.txt'])],
      # packages=['importer'],
      cmdclass = {
                  'clean': CleanCommand
                  },
      install_requires=[
        "boto3==1.7.45",
        "botocore==1.10.45",
        "click==6.7",
        "docutils==0.14",
        "jmespath==0.9.3",
        "mysql-connector==2.2.9",
        "mysqlclient==1.3.12",
        "pandas==0.23.3",
        "python-dateutil==2.7.3",
        "s3transfer==0.1.13",
        "six==1.11.0",
        "selenium==3.141.0",
        "beautifulsoup4==4.6.0"
      ],
      zip_safe=False)
