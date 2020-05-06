"""Reads parquet files on disk and then resamples them to 1 minute data files and joins into a single file to make it
quick to load for other purposes (eg. backtesting), also makes USD base currency to make it easier to calculate cross-rates

Note, we need to be careful when calculating cross-rates from data which is not regularly spaced.
"""

from __future__ import print_function, division

__author__ = 'saeedamen'  # Saeed Amen / saeed@cuemacro.com

#
# Copyright 2020 Cuemacro
#
# See the License for the specific language governing permissions and limitations under the License.
#

import pandas as pd
import dask.dataframe as dd

from findatapy.timeseries.calculations import Calculations

from tcapy.util.loggermanager import LoggerManager
from tcapy.util.utilfunc import UtilFunc

util_func = UtilFunc()
calculations = Calculations()

data_vendor = 'dukascopy'
file_extension = 'parquet'
resample_freq = '1min'

csv_folder = '/home/tcapyuser/csv_dump/' + data_vendor + '/'
csv_output = '/home/tcapyuser/csv_output/'

combined_file = 'fx_' + resample_freq + '_' + data_vendor + '.' + file_extension

ticker_mkt = ['EURUSD', 'GBPUSD', 'AUDUSD', 'NZDUSD', 'USDCAD', 'USDCHF', 'EURNOK', 'EURSEK', 'USDJPY']

ticker_mkt = ['EURUSD', 'GBPUSD', 'AUDUSD', 'NZDUSD', 'USDCAD', 'USDJPY']
ticker_combined_mkt = ['EURUSD', 'GBPUSD', 'AUDUSD', 'NZDUSD', 'USDCAD', 'USDJPY']

def create_resampled_spot_data():
    logger = LoggerManager.getLogger(__name__)

    for ticker in ticker_mkt:

        logger.info("Processing for " + ticker)

        flat_file = csv_folder + ticker + '_' + data_vendor + '_*.' + file_extension

        df_dd = dd.read_parquet(flat_file).compute()['mid']

        logger.info("About to resample OHLC for " + ticker)

        df_dd_ohlc = df_dd.resample(resample_freq).ohlc()

        print(df_dd_ohlc.columns)

        logger.info("About to resample count for " + ticker)
        df_dd_count = df_dd.resample(resample_freq).count()
        df_dd_count.columns = ['tickcount']

        df_dd = df_dd_ohlc.join(df_dd_count)
        df_dd.columns = [ticker + '.' + x for x in df_dd.columns]

        df_dd = df_dd.dropna()
        df_dd.to_parquet(csv_output + ticker + '_' + resample_freq + '_' + data_vendor + '.' + file_extension)

def combine_resampled_spot_data_into_single_dataframe_usd_base():
    df_list = []

    logger = LoggerManager.getLogger(__name__)

    for ticker in ticker_combined_mkt:
        logger.info("Reading " + ticker)

        df = pd.read_parquet(csv_output + ticker + '_' + resample_freq + '_' + data_vendor + '.' + file_extension)

        base = ticker[0:3]
        terms = ticker[3:6]

        if terms == 'USD':
            df_invert = pd.DataFrame(index=df.index)
            df_invert[terms + base + '.close'] = 1.0 / df[ticker + '.close']
            df_invert[terms + base + '.open'] = 1.0 / df[ticker + '.open']

            # Invert high and low!
            df_invert[terms + base + '.high'] = 1.0 / df[ticker + '.low']
            df_invert[terms + base + '.low'] = 1.0 / df[ticker + '.high']

            df_invert[terms + base + '.close'] = 1.0 / df[ticker + '.close']

            df = df_invert

        df_list.append(df)

    df = pd.DataFrame(index=df.index)

    df['USDUSD.close'] = 1.0

    df_list.append(df)
    df = calculations.pandas_outer_join(df_list)
    df = df.dropna()

    df.to_parquet(csv_output + combined_file)

if __name__ == '__main__':
    create_resampled_spot_data()
    combine_resampled_spot_data_into_single_dataframe_usd_base()



