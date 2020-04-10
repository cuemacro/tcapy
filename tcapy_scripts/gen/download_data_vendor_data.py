"""Calls data vendor via API and displays market data output
"""

from __future__ import print_function, division

__author__ = 'saeedamen'  # Saeed Amen / saeed@cuemacro.com

#
# Copyright 2017 Cuemacro Ltd. - http//www.cuemacro.com / @cuemacro
#
# See the License for the specific language governing permissions and limitations under the License.
#

from tcapy.data.databasesource import DatabaseSourceDukascopy as DatabaseSource

if __name__ == '__main__':

    import time

    start = time.clock()

    database_source = DatabaseSource()

    ticker = 'EURUSD'

    df1 = database_source.fetch_market_data(start_date='05 Aug 2019', finish_date='06 Aug 2019', ticker=ticker)
    df2 = database_source.fetch_market_data(start_date='06 Aug 2019', finish_date='07 Aug 2019', ticker=ticker)
    df3 = database_source.fetch_market_data(start_date='07 Aug 2019', finish_date='08 Aug 2019', ticker=ticker)

    print(df1)
    print(df2)
    print(df3)

    finish = time.clock()
    print('Status: calculated ' + str(round(finish - start, 3)) + "s")
