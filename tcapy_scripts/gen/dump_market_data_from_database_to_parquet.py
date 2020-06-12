"""Script to fetch market data from database and dump to disk as a Parquet.
This can be useful if we want to transfer the data to another computer later, or for backup purposes.
"""

from __future__ import division, print_function

__author__ = 'saeedamen'  # Saeed Amen / saeed@cuemacro.com

#
# Copyright 2020 Cuemacro Ltd. - http//www.cuemacro.com / @cuemacro
#
# See the License for the specific language governing permissions and limitations under the License.
#

import os
from tcapy.conf.constants import Constants

constants = Constants()

if __name__ == '__main__':
    # 'arctic' or 'pystore'
    database_dialect = 'arctic'

    # 'dukascopy' or 'ncfx'
    data_vendor = 'dukascopy'

    # Where to dump the CSV (& Parquet) files - make sure this exists
    folder = '/home/tcapyuser/debug_tick/'

    # Warning for high frequency data file sizes might be very big, so you may need to reduce this!
    start_date = '01 Jan 2020'; finish_date = '01 Jun 2020'

    tickers = ['EURUSD', 'GBPUSD', 'AUDUSD', 'NZDUSD', 'USDCAD', 'USDCHF', 'EURNOK', 'EURSEK', 'USDJPY']

    if database_dialect == 'arctic':
        from tcapy.data.databasesource import DatabaseSourceArctic as DatabaseSource
    elif database_dialect == 'pystore':
        from tcapy.data.databasesource import DatabaseSourcePyStore as DatabaseSource
    elif database_dialect == 'influxdb':
        from tcapy.data.databasesource import DatabaseSourceInfluxDB as DatabaseSource

    database_source = DatabaseSource(postfix=data_vendor)

    file_format = 'parquet'

    for t in tickers:
        market_df = database_source.fetch_market_data(start_date=start_date, finish_date=finish_date, ticker=t)

        key = '_' + data_vendor + "_" + \
              (str(market_df.index[0]) + str(market_df.index[-1])).replace(":", '_').replace(" ", '_')
        filename = os.path.join(folder, t + key) + '.' + file_format

        if market_df is not None:
            c = market_df.columns
            print('Writing ' + t + ' to ' + filename)
            print('No of items ' + str(len(market_df.index)))

            if file_format == 'parquet':
                market_df.to_parquet(filename)
            elif file_format == 'csv':
                market_df.to_csv(filename)
