from setuptools import setup, find_packages

from tcapy import __version__ as version

long_description = """tcapy is a Python library for doing transaction cost analysis (TCA), essentially finding the cost of your trading activity.
Across the industry many financial firms and corporates trading within financial markets spend a lot of money on TCA, either
by developing in house tools or using external services. It is estimated that the typical buy side equities trading desk
spends around 225k USD a year on TCA (see MarketsMedia report at https://www.marketsmedia.com/tca-growth-fueled-regulators-investors/).
Many sell side firms and larger buy side firms build and maintain their own TCA libraries, which is very expensive. The cost of TCA
across the industry is likely to run into many hundreds of millions of dollars or possibly billions of dollars.

Much of the complexity in TCA is due to the need to handle large tick datasets and do calculations on them and is largely a
software engineering problem. This work needs to be repeated in every single implementation. By open sourcing the library
we hope that the industry will no longer need to keep reinventing the wheel when it comes to TCA. At the same time,
because all the code is visible to users, tcapy allows you can add your own customized metrics and benchmarks,
which is where you are likely have very particular IP in financial markets. You get the flexibility of a fully internal
TCA solution for free.

tcapy is one of the first open source libraries for TCA. You can run the library on your own hardware, so your trade/order
data can be kept private. It has been in development since June 2017, originally for a large asset manager and was open
sourced in March 2020.

We've made tcapy to be vendor independent, hence for example it supports, multiple database types for storing
market tick data (including Arctic/MongoDB, KDB and InfluxDB) and for trade/order data (including MySQL, PostgreSQL and
Microsoft SQL Server). As well as supporting Linux (tested on Ubuntu/Red Hat) - also it also works on Windows 
(need Windows Subsystem for Linux to make all features accessible)

tcapy has also been written to distribute the computation and make a lot of use of caching. In the future, we are hoping to
add features to make it easy to use serverless computing features on the cloud. Since you can see all the code, it also
makes the TCA totally transparent. If you are doing TCA for regulatory reasons, it makes sense that the process should
be fully open, rather than a black box. Having an open source library, makes it easier to make changes and fitting it to your
use case.
"""

with open('requirements.txt') as f:
    install_requires = f.read()

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
      install_requires=install_requires,
      zip_safe=False)
