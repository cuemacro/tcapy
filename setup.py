from setuptools import setup, find_packages

from tcapy import __version__ as version

# read the contents of your README file
with open('README.md') as f:
    long_description = f.read()

#long_description = """tcapy is a transaction cost analysis library for determining calculating your trading costs"""

setup(name='tcapy',
      version=version,
      description='Tranasction cost analysis library',
      author='Saeed Amen',
      author_email='saeed@cuemacro.com',
      license='Apache 2.0',
      long_description=long_description,
      keywords=['pandas', 'TCA', 'transaction cost analysis'],
      url='https://github.com/cuemacro/tcapy',
      #packages=find_packages(),
      packages=find_packages(include=["tcapy*"]),
      include_package_data=True,
      install_requires=['pandas>=0.25.3','arctic','scipy','plotly','numba','sqlalchemy','statsmodels','flask','pystore', 'dask[dataframe]','fsspec>=0.3.3','influxdb'],
      zip_safe=False)
