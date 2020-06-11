from setuptools import setup, find_packages

from tcapy import __version__ as version

# read the contents of your README file
with open('README.md') as f:
    long_description = f.read()

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
