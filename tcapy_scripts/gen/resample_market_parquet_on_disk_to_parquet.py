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

file_extension = 'parquet'

csv_output = '/data/csv_output/'

ticker_mkt = ['EURUSD', 'GBPUSD', 'AUDUSD', 'NZDUSD', 'USDCAD', 'USDCHF', 'EURNOK', 'EURSEK', 'USDJPY']

ticker_combined_mkt = ['EURUSD', 'GBPUSD', 'AUDUSD', 'NZDUSD', 'USDCAD', 'USDJPY']

def create_resampled_spot_data(resample_freq='1min', data_vendor='dukascopy'):

    logger = LoggerManager.getLogger(__name__)
    csv_input_folder = '/data/csv_dump/' + data_vendor + '/'

    for ticker in ticker_mkt:

        logger.info("Processing for " + ticker  + " resample freq " + resample_freq + " data vendor " + data_vendor)

        flat_file = csv_input_folder + ticker + '_' + data_vendor + '_*.' + file_extension

        df_dd = dd.read_parquet(flat_file).compute()['mid']

        logger.info("About to resample OHLC for " + ticker + " resample freq " + resample_freq + " data vendor " + data_vendor)

        resampler = df_dd.resample(resample_freq)
        df_dd_ohlc = resampler.ohlc()

        print(df_dd_ohlc.columns)

        logger.info("About to resample count for " + ticker)
        df_dd_count = resampler.count()
        df_dd_count.name = 'tickcount'

        df_dd = pd.concat([df_dd_ohlc, df_dd_count], axis=1)
        df_dd.columns = [ticker + '.' + x for x in df_dd.columns]

        df_dd = df_dd.dropna()
        df_dd.to_parquet(csv_output + ticker + '_' + resample_freq + '_' + data_vendor + '.' + file_extension)

        df_dd = None

def combine_resampled_spot_data_into_single_dataframe_usd_base(resample_freq='1min', data_vendor='dukascopy'):
    df_list = []

    logger = LoggerManager.getLogger(__name__)

    for ticker in ticker_combined_mkt:
        logger.info("Reading " + ticker + " resample freq " + resample_freq + " data vendor " + data_vendor)

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

            df_invert[terms + base + '.tickcount'] = df[ticker + '.tickcount']

            df = df_invert

        df_list.append(df)

    logger.info("Combining all tickers with resample freq " + resample_freq + " data vendor " + data_vendor)
    df = pd.DataFrame(index=df.index)

    df['USDUSD.close'] = 1.0

    df_list.append(df)
    df = calculations.pandas_outer_join(df_list)
    df = df.dropna()

    combined_file = 'fx_' + resample_freq + '_' + data_vendor + '.' + file_extension

    df.to_parquet(csv_output + combined_file)

if __name__ == '__main__':
    data_vendor_list = ['dukascopy']
    resample_freq_list = ['1min', '1s']

    for data_vendor in data_vendor_list:
        for resample_freq in resample_freq_list:
            create_resampled_spot_data(resample_freq=resample_freq, data_vendor=data_vendor)
            combine_resampled_spot_data_into_single_dataframe_usd_base(resample_freq=resample_freq, data_vendor=data_vendor)


