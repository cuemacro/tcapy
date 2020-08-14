from __future__ import print_function

__author__ = 'saeedamen'  # Saeed Amen / saeed@cuemacro.com

#
# Copyright 2020 Cuemacro Ltd. - http//www.cuemacro.com / @cuemacro
#
# See the License for the specific language governing permissions and limitations under the License.
#

import os

from tcapy.util.loggermanager import LoggerManager
from tcapy.data.databasesource import AccessControl, DatabaseSourceArctic, DatabaseSourcePyStore, DatabaseSourceInfluxDB, \
    DatabaseSourceKDB

from tcapy.conf.constants import Constants

constants = Constants()

class DataDumper(object):
    """Provides convenient methods from uploading trade data stored in flat files such as CSV or Parquet to dump into
    SQL databases for use by tcapy. Also has similar methods for market data.

    """

    def upload_market_data_flat_file(self, data_vendor='dukascopy', market_data_store='arctic', server_host=None,
                                     server_port=None,
                                     ticker_mkt=['EURUSD', 'GBPUSD', 'AUDUSD', 'NZDUSD', 'USDCAD', 'USDCHF',
                                                 'EURNOK', 'EURSEK', 'USDJPY',
                                                 'USDNOK', 'USDSEK', 'EURJPY',
                                                 'USDMXN', 'USDTRY', 'USDZAR', 'EURPLN'],
                                     csv_folder=None,
                                     if_exists_table='replace',
                                     if_append_replace_ticker='replace',
                                     file_extension='parquet',
                                     read_in_reverse=False, remove_duplicates=False, date_format=None,
                                     plot_back_data=False, access_control=AccessControl(), market_data_database_table=None):

        logger = LoggerManager.getLogger(__name__)

        # Files dumped by DatabasePopulator look like this
        ## 'AUDUSD_dukascopy_2016-01-03_22_00_01.868000+00_002016-01-31_23_59_57.193000+00_00.parquet'
        csv_file = [x + '_' + data_vendor + '_20*.' + file_extension for x in
                    ticker_mkt]  # assume that ALL TIME IN UTC!

        ####################################################################################################################

        # Load market data

        # Create market data store for database and associated data vendor
        if market_data_store == 'arctic':
            if server_host is None:
                server_host = constants.arctic_host

            if server_port is None:
                server_port = constants.arctic_port

            database_source = DatabaseSourceArctic(postfix=data_vendor,
                                                username=access_control.arctic_username,
                                                password=access_control.arctic_password,
                                                server_host=server_host, server_port=server_port)

            if market_data_database_table is None:
                market_data_database_table = constants.arctic_market_data_database_table

        if market_data_store == 'pystore':
            database_source = DatabaseSourcePyStore(postfix=data_vendor)

            if market_data_database_table is None:
                market_data_database_table = constants.pystore_market_data_database_table

        if market_data_store == 'influxdb':
            if server_host is None:
                server_host = constants.influxdb_host

            if server_port is None:
                server_port = constants.influxdb_port

            database_source = DatabaseSourceInfluxDB(postfix=data_vendor,
                                                     username=access_control.influxdb_username,
                                                     password=access_control.influxdb_password,
                                                     server_host=server_host, server_port=server_port)

            if market_data_database_table is None:
                market_data_database_table = constants.influxdb_market_data_database_table

        if market_data_store == 'kdb':
            if server_host is None:
                server_host = constants.kdb_host

            if server_port is None:
                server_port = constants.kdb_port

            database_source = DatabaseSourceKDB(postfix=data_vendor,
                                                username=access_control.kdb_username,
                                                password=access_control.kdb_password,
                                                server_host=server_host, server_port=server_port)

            if market_data_database_table is None:
                market_data_database_table = constants.kdb_market_data_database_table

        if csv_folder is None:
            csv_folder = constants.test_data_folder

        # This relies on you have market data stored in Parquet/H5/CSV files already (eg. by downloading from DukasCopy)
        # note: whilst free FX data can be used for testing (in particular for generating randomised trades),
        # you may to choose other high frequency quality data for actual benchmark

        csv_market_data = [os.path.join(csv_folder, x) for x in csv_file]

        # For each ticker, read in the H5/CSV file and then dump into tick database
        # Potentionally, we can thread this?
        for i in range(0, len(ticker_mkt)):
            ticker = ticker_mkt[i]
            csv_file = csv_market_data[i]

            # On the second time through the loop, we make sure to append to table
            # otherwise will keep overwriting!
            if if_exists_table == 'replace':
                if i >= 1:
                    if_exists_table = 'append'
                else:
                    if_exists_table = 'replace'

            database_source.convert_csv_to_table(csv_file, ticker, market_data_database_table,
                                                 if_exists_table=if_exists_table,
                                                 remove_duplicates=remove_duplicates,
                                                 if_exists_ticker=if_append_replace_ticker, date_format=date_format,
                                                 read_in_reverse=read_in_reverse)

            # It is worth plotting the data to check validity sometimes (make sure you choose appropriate start/finish dates
            # loading a *very* large tick history into memory will result in your computer running out of memory
            if plot_back_data:
                from chartpy import Chart

                import datetime
                import pandas as pd

                df = database_source.fetch_market_data(start_date='01 Jan 2000',
                                                       finish_date=datetime.datetime.utcnow(),
                                                       ticker=ticker)

                df = pd.DataFrame(df.resample('5min').mean())

                if 'mid' not in df.columns:
                    df['mid'] = (df['bid'] + df['ask']) / 2.0

                df = pd.DataFrame(df['mid'])

                Chart(engine='plotly').plot(df)

                print(df)

        logger.info("Finished uploading data to " + market_data_store)

    def upload_trade_data_flat_file(self, sql_database_type=None, trade_data_database_name=None,
                                    csv_sql_table_trade_order_mapping=None,
                                    server_host=None, server_port=None, if_exists_trade_table='replace',
                                    access_control=AccessControl()):

        if sql_database_type == 'ms_sql_server':
            from tcapy.data.databasesource import DatabaseSourceMSSQLServer as DatabaseSource

            username = access_control.ms_sql_server_username; password = access_control.ms_sql_server_password
        elif sql_database_type == 'mysql':
            from tcapy.data.databasesource import DatabaseSourceMySQL as DatabaseSource

            username = access_control.mysql_username; password = access_control.mysql_password
        elif sql_database_type == 'sqlite':
            from tcapy.data.databasesource import DatabaseSourceSQLite as DatabaseSource

            username = access_control.ms_sql_server_username;
            password = access_control.ms_sql_server_password

        if server_host is None and server_port is None:
            database_source = DatabaseSource(trade_data_database_name=trade_data_database_name,
                                             username=username, password=password)
        elif server_port is None:
            database_source = DatabaseSource(trade_data_database_name=trade_data_database_name,
                                             server_host=server_host,
                                             username=username, password=password)
        elif server_host is None:
            database_source = DatabaseSource(trade_data_database_name=trade_data_database_name,
                                             server_port=server_port,
                                             username=username, password=password)
        else:
            database_source = DatabaseSource(trade_data_database_name=trade_data_database_name,
                                             server_host=server_host, server_port=server_port,
                                             username=username, password=password)

        for key in csv_sql_table_trade_order_mapping.keys():
            database_source.convert_csv_to_table(
                csv_sql_table_trade_order_mapping[key], None, key, database_name=trade_data_database_name,
                if_exists_table=if_exists_trade_table)
