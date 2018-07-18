from setuptools import setup
 
# setup(
#     name='importer',    # This is the name of your PyPI-package.
#     version='0.1',                          # Update the version number for new releases
#     scripts=['runner-import']                  # The name of your scipt, and also the command you'll be using for calling it
# )

setup(name='importer',
      version='0.1',
      description='Importer',
      scripts=['./runner-import.py',],
      packages=['importer.loaders', 'importer.sql'],
      install_requires=[
        "boto3==1.7.45",
        "botocore==1.10.45",
        "click==6.7",
        "docutils==0.14",
        "jmespath==0.9.3",
        "mysql-connector==2.1.6",
        "mysqlclient==1.3.12",
        "python-dateutil==2.7.3",
        "s3transfer==0.1.13",
        "six==1.11.0"
      ],
      zip_safe=False)
