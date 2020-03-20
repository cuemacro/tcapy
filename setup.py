from setuptools import setup, find_packages

long_description = """tcapy is a transaction cost analysis library for determining calculating your trading costs"""

setup(name='tcapy',
      version='0.1.0',
      description='Tranasction cost analysis library',
      author='Saeed Amen',
      author_email='saeed@cuemacro.com',
      license='Apache 2.0',
      long_description=long_description,
      keywords=['pandas', 'TCA', 'transaction cost analysis'],
      url='https://github.com/cuemacro/tcapy',
      packages=find_packages(),
      include_package_data=True,
      install_requires=[],
      zip_safe=False)
