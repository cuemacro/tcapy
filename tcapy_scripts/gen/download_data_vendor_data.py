"""Calls data vendor via API and displays market data output
"""

from __future__ import print_function, division

__author__ = 'saeedamen'  # Saeed Amen / saeed@cuemacro.com

#
# Copyright 2017 Cuemacro Ltd. - http//www.cuemacro.com / @cuemacro
#
# See the License for the specific language governing permissions and limitations under the License.
#

if __name__ == '__main__':

    import time

    start = time.clock()

    data_vendor = 'ncfx'

    if data_vendor == 'dukascopy':
        from tcapy.data.databasesource import DatabaseSourceDukascopy as DatabaseSource
    elif data_vendor == 'ncfx':
        from tcapy.data.databasesource import DatabaseSourceNCFX as DatabaseSource

    database_source = DatabaseSource()

    ticker = 'EURUSD'

    # Note: some data sources might only let you download a small chunk of tick data (try daily and hourly)
    df1 = database_source.fetch_market_data(start_date='05 Aug 2019 00:00', finish_date='05 Aug 2019 02:00', ticker=ticker)
    df2 = database_source.fetch_market_data(start_date='06 Aug 2019', finish_date='07 Aug 2019', ticker=ticker)
    df3 = database_source.fetch_market_data(start_date='07 Aug 2019', finish_date='08 Aug 2019', ticker=ticker)

    # Both points on a Saturday (should result in empty DataFrame)
    df4 = database_source.fetch_market_data(start_date='25 Apr 2020 00:00', finish_date='25 Apr 2020 02:00',
                                            ticker=ticker)

    # Start late on Friday and finish on Saturday (should have some data)
    df5 = database_source.fetch_market_data(start_date='24 Apr 2020 18:00', finish_date='25 Apr 2020 02:00',
                                            ticker=ticker)


    print(df1)
    print(df2)
    print(df3)
    print(df4)
    print(df5)

    finish = time.clock()
    print('Status: calculated ' + str(round(finish - start, 3)) + "s")
