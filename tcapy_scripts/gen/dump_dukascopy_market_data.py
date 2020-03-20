"""Script to download DukasCopy FX tick data and dump it to disk as HDF5 files. This tick data can be used to test the
application. Typically, we can then copy this dataset into Arctic. Uses findatapy to download the data.
Other tick data is available through findatapy including FXCM.

Note, we can also
"""

from __future__ import division, print_function

__author__ = 'saeedamen'  # Saeed Amen / saeed@cuemacro.com

#
# Copyright 2018 Cuemacro Ltd. - http//www.cuemacro.com / @cuemacro
#
# See the License for the specific language governing permissions and limitations under the License.
#

import datetime
from datetime import timedelta
import random
import pandas

from tcapy.util.loggermanager import LoggerManager

import pickle as pkl

class MarketTestData(object):
    """Wrapper for findatapy to download FX spot data from DukasCopy retail broker, and then dump to disk in HDF5 format.
    These HDF5 flatfiles can be reused later. This data can be used to test the tcapy application (although other FX
    tick data sources can be used).

    If you want to use this to download DukasCopy, you must first install findatapy (https://github.com/cuemacro/findatapy)
    and its Python dependencies including the Python packages quandl, alpha_vantage, openpyxl, numba etc.

    Note, the Bloomberg dependency for findatapy does not need to be installed, given we are not downloading Bloomberg data for this.

    For DukasCopy in particular backports.lzma must be installed (for Python 2) - see https://pypi.org/project/backports.lzma/ for details
    - on how to install this Python package, which requires several dependencies which varies between operating systems)

    For installation of backports.lzma on Windows, it is easier to use a precompiled version from
    https://www.lfd.uci.edu/~gohlke/pythonlibs/#backports.lzma and pip install that one
    """

    def __init__(self):
        pass

    def create_test_raw_data(self, ticker_list=None, start_date=None, finish_date=None, folder_prefix=None):
        """Downloads FX tick data from DukasCopy and then dumps each ticker in a separate HDF5 file if a folder is specified.
        If no folder is specified returns a list of DataFrames (note: can be a very large list in memory)

        Parameters
        ----------
        ticker_list : str (list)
            List of FX tickers to download

        start_date : datetime/str
            Start date of FX tick data download

        finish_date : datetime/str
            Finish date of FX tick data download

        folder_prefix : str
            Folder to dump everything

        Returns
        -------
        DataFrame (list)
        """

        from findatapy.market import MarketDataRequest, MarketDataGenerator, Market

        if start_date is None and finish_date is None:
            finish_date = datetime.datetime.utcnow().date() - timedelta(days=30)
            start_date = finish_date - timedelta(days=30 * 15)

            start_date = self._compute_random_date(start_date, finish_date)
            finish_date = start_date + timedelta(days=90)

        df_list = []
        result = []

        # From multiprocessing.dummy import Pool # threading
        from multiprocess.pool import Pool # actuall new processes
        import time

        # If we don't specify a folder
        if folder_prefix is None:

            mini_ticker_list = self._split_list(ticker_list, 2)

            # Use multiprocess to speed up the download
            for mini in mini_ticker_list:
                pool = Pool(processes=2)

                for ticker in mini:
                    time.sleep(1)
                    self.logger.info("Loading " + ticker)
                    md_request = MarketDataRequest(start_date=start_date, finish_date=finish_date, category='fx',
                                                   tickers=ticker, fields=['bid', 'ask', 'bidv', 'askv'],
                                                   data_source='dukascopy', freq='tick')

                    # self._download(md_request)
                    result.append(pool.apply_async(self._download, args=(md_request, folder_prefix,)))

                pool.close()
                pool.join()

        else:
            market = Market(market_data_generator=MarketDataGenerator())

            for ticker in ticker_list:

                md_request = MarketDataRequest(start_date=start_date, finish_date=finish_date, category='fx',
                                               tickers=ticker, fields=['bid', 'ask', 'bidv', 'askv'],
                                               data_source='dukascopy', freq='tick')

                df = market.fetch_market(md_request=md_request)

                df.columns = ['bid', 'ask', 'bidv', 'askv']

                df['venue'] = 'dukascopy'
                df['ticker'] = ticker

                # print(df)

                if folder_prefix is not None:
                    self.dump_hdf5_file(df, folder_prefix + "_" + ticker + ".h5")
                    # df.to_csv(folder_prefix + "_" + ticker + ".csv") # CSV files can be very large, so try to avoid
                else:
                    df_list.append(df)

            return df_list

    def _download(self, md_request, folder_prefix):
        from findatapy.market import MarketDataRequest, MarketDataGenerator, Market
        
        logger = LoggerManager.getLogger(__name__)
        market = Market(market_data_generator=MarketDataGenerator())

        ticker = md_request.ticker[0]
        df = market.fetch_market(md_request=md_request)

        df.columns = ['bid', 'ask', 'bidv', 'askv']

        df['venue'] = 'dukascopy'
        df['ticker'] = ticker

        df['mid'] = (df['bid'].values + df['ask'].values) / 2.0

        self.dump_hdf5_file(df, folder_prefix + "_" + ticker + ".h5")

        logger.info('Dumped to ' + folder_prefix + "_" + ticker + ".h5")

    def _chunk(self, input, size):
        return list(map(None, *([iter(input)] * size)))

    def _split_list(self, list, chunk_size):
        return [list[offs:offs + chunk_size] for offs in range(0, len(list), chunk_size)]

    def _compute_random_date(self, start, finish):
        return start + datetime.timedelta(
            # Get a random amount of seconds between `start` and `end`
            seconds=random.randint(0, int((finish - start).total_seconds())),
        )

    def dump_hdf5_file(self, df, filename):
        """Dump DataFrame to disk as HDF5 data. Note, you may need to remove the compression in some instances.

        Parameters
        ----------
        df : DataFrame
            Market data to be dumped

        filename : str
            Path of filename
        """

        store = pandas.HDFStore(filename, format='fixed', complib="blosc", complevel=9)

        store.put(key='data', value=df, format='fixed')
        store.close()

    def read_hdf5_file(self, filename):
        """Reads an HDF5 dumped DataFrame from disk

        Parameters
        ----------
        filename : str
            Path of HDF5 based DataFrame

        Returns
        -------
        DataFrame
        """

        store = pandas.HDFStore(filename)
        data_frame = store.select("data")
        store.close()

        return data_frame


if __name__ == '__main__':

    market_test_data = MarketTestData()

    # folder_prefix = 'E://tcapy_tests_data//dukascopy//test_market'

    # change as appropriate
    folder_prefix = '/tmp/dk/test_market'

    pkl.HIGHEST_PROTOCOL = 2 # so compatible with Python 2.7

    LARGE_SAMPLE = False
    SMALL_SAMPLE = True
    READ_DF = True

    if LARGE_SAMPLE:
        ticker_list = ['EURUSD', 'GBPUSD', 'AUDUSD', 'NZDUSD', 'USDCAD', 'USDCHF', 'USDNOK', 'USDSEK', 'USDJPY',
                       'USDTRY', 'USDZAR', 'EURSEK', 'EURNOK']

        start_date = '01 Jan 2016'; finish_date = '01 May 2018'

        market_test_data.create_test_raw_data(ticker_list=ticker_list, start_date=start_date, finish_date=finish_date,
                                              folder_prefix=folder_prefix)

    if SMALL_SAMPLE:
        ticker_list = ['EURUSD', 'USDJPY']

        # start_date = '25 Apr 2017'; finish_date = '05 Jun 2017'
        start_date = '25 Apr 2017'; finish_date = '27 Apr 2017'

        market_test_data.create_test_raw_data(ticker_list=ticker_list, start_date=start_date, finish_date=finish_date,
                                              folder_prefix=folder_prefix)

    if READ_DF:
        print(market_test_data.read_hdf5_file(folder_prefix + '_EURUSD.h5'))
