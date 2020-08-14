"""Can be used to populate the market and trade/order databases. Populate market database with market data in Parquet/H5/CSVs (Arctic).
Users can modify the Parquet/H5/CSV paths, so they can dump their own trade/order data into the trade database.

Uses DataDumper underneath to access the various databases (via DatabaseSource)
"""

from __future__ import print_function, division

__author__ = 'saeedamen'  # Saeed Amen / saeed@cuemacro.com

#
# Copyright 2018 Cuemacro
#
# See the License for the specific language governing permissions and limitations under the License.
#

from tcapy.util.loggermanager import LoggerManager

if __name__ == '__main__':
    logger = LoggerManager.getLogger(__name__)

    plot_back_data = False
    data_vendor = 'dukascopy'  # 'dukascopy' or 'ncfx'

    # Either use 'arctic' or 'pystore' or 'influxdb' or 'kdb' to store market tick data
    market_data_store = 'arctic'

    # If left as None, will pick up from constants
    server_host = None
    server_port = None

    logger.info("About to upload data to " + market_data_store)

    ## YOU WILL NEED TO CHANGE THE BELOW LINES #########################################################################

    # dukascopy or ncfx style parameters for uploading a large number of Parquet files with market data
    # Note: use of wildcard * to specify multiple files

    ticker_mkt = ['EURUSD', 'GBPUSD', 'AUDUSD', 'NZDUSD', 'USDCAD', 'USDCHF',
                  'EURNOK', 'EURSEK', 'USDJPY',
                  'USDNOK', 'USDSEK', 'EURJPY',
                  'USDMXN', 'USDTRY', 'USDZAR', 'EURPLN']

    csv_folder = '/data/csv_dump/' + data_vendor + '/'

    if_exists_table = 'replace'  # 'replace' or 'append' to database table
    if_append_replace_ticker = 'replace'  # 'replace' or 'append' to ticker

    file_extension = 'parquet'  # 'parquet' (recommended) or 'csv' or 'h5' on disk

    # Files dumped by DatabasePopulator look like this
    ## 'AUDUSD_dukascopy_2016-01-03_22_00_01.868000+00_002016-01-31_23_59_57.193000+00_00.parquet'

    from tcapy.data.datadumper import DataDumper

    data_dumper = DataDumper()
    data_dumper.upload_market_data_flat_file(data_vendor=data_vendor, market_data_store=market_data_store,
                                             server_host=server_host, server_port=server_port,
                                             ticker_mkt=ticker_mkt,
                                             csv_folder=csv_folder,
                                             if_exists_table=if_exists_table,
                                             if_append_replace_ticker=if_append_replace_ticker,
                                             file_extension=file_extension,
                                             plot_back_data=plot_back_data)




