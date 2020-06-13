"""Tests various time series functions which are used extensively in tcapy
"""

__author__ = 'saeedamen'  # Saeed Amen / saeed@cuemacro.com

#
# Copyright 2017 Cuemacro Ltd. - http//www.cuemacro.com / @cuemacro
#
# See the License for the specific language governing permissions and limitations under the License.
#

import pandas as pd
import numpy as np
from datetime import timedelta
from pandas.testing import assert_frame_equal

from tcapy.util.timeseries import TimeSeriesOps

from tcapy.util.customexceptions import *

from test.config import *

ticker = 'EURUSD'
start_date = '20 Apr 2017'
finish_date = '07 Jun 2017'

def test_vlookup():
    """Runs a test for the VLOOKUP function which is used extensively in a lot of the metric construction
    """

    dt = pd.date_range(start='01 Jan 2018', end='05 Jan 2018', freq='1min')

    rand_data = np.random.random(len(dt))

    df_before = pd.DataFrame(index=dt, columns=['rand'], data=rand_data)

    millseconds_tests = [100, 500]

    # Try perturbing by nothing, then 100 and 500 milliseconds
    for millseconds in millseconds_tests:
        df_perturb = pd.DataFrame(index=dt - timedelta(milliseconds=millseconds), columns=['rand'],
                                      data=rand_data)

        # Do a VLOOKUP (which should give us all the previous ones) - take off the last point (which would be AFTER
        # our perturbation)
        search, dt_search = TimeSeriesOps().vlookup_style_data_frame(dt[0:-1], df_perturb, 'rand')

        df_after = pd.DataFrame(index=dt_search + timedelta(milliseconds=millseconds), data=search.values,
                                    columns=['rand'])

        # check the search dataframes are equal
        assert_frame_equal(df_before[0:-1], df_after, check_dtype=False)

    # in this case, our lookup series doesn't overlap at all with our range, so we should get back and exception
    dt_lookup = pd.date_range(start='30 Dec 2017', end='31 Dec 2018', freq='1min')

    df_perturb = pd.DataFrame(index=dt + timedelta(milliseconds=millseconds), columns=['rand'],
                                  data=rand_data)

    exception_has_been_triggered = False

    try:
        search, dt_search = TimeSeriesOps().vlookup_style_data_frame(dt_lookup, df_perturb, 'rand')
    except ValidationException:
        exception_has_been_triggered = True

    assert (exception_has_been_triggered)

def test_filter_between_days_times():
    """Runs a test for the filter by time of day and day of the week, on synthetically constructed data and then checks
    that no data is outside those time windows
    """
    from tcapy.analysis.tradeorderfilter import TradeOrderFilterTimeOfDayWeekMonth

    dt = pd.date_range(start='01 Jan 2018', end='05 Jan 2018', freq='1min')

    df = pd.DataFrame(index=dt, columns=['Rand'], data=np.random.random(len(dt)))
    df = df.tz_localize('utc')

    trade_order_filter = TradeOrderFilterTimeOfDayWeekMonth(time_of_day={'start_time': '07:00:00',
                                                                         'finish_time': '17:00:00'},
                                                            day_of_week='mon')
    df = trade_order_filter.filter_trade_order(trade_order_df=df)

    assert (df.index[0].hour >= 7 and df.index[-1].hour <= 17 and df.index[0].dayofweek == 0)

def test_remove_consecutive_duplicates():
    """Tests that consecutive duplicates are removed correctly in time series
    """
    dt = pd.date_range(start='01 Jan 2018', end='05 Jan 2018', freq='30s')

    df = pd.DataFrame(index=dt, columns=['bid', 'mid', 'ask'])

    df['mid'] = np.random.random(len(dt))
    df['bid'] = np.random.random(len(dt))
    df['ask'] = np.random.random(len(dt))

    # Filter by 'mid'
    df2 = df.copy()

    df2.index = df2.index + timedelta(seconds=10)

    df_new = df.append(df2)
    df_new = df_new.sort_index()

    df_new = TimeSeriesOps().drop_consecutive_duplicates(df_new, 'mid')

    assert_frame_equal(df_new, df)

    # For 'bid' and 'ask'
    df2 = df.copy()

    df2.index = df2.index + timedelta(seconds=10)

    df_new = df.append(df2)
    df_new = df_new.sort_index()

    df_new = TimeSeriesOps().drop_consecutive_duplicates(df_new, ['bid', 'ask'])

    assert_frame_equal(df_new, df)

def test_ohlc():
    """Tests the open/high/low/close resampling works on time series
    """
    dt = pd.date_range(start='01 Jan 2018', end='05 Jan 2018', freq='1s')

    df = pd.DataFrame(index=dt, columns=['bid', 'mid', 'ask'])

    df['mid'] = np.random.random(len(dt))

    df_ohlc = TimeSeriesOps().resample_time_series(df, resample_amount=1, how='ohlc', unit='minutes', field='mid')

    assert all(df_ohlc['high'] >= df_ohlc['low'])


def test_time_delta():
    """Tests time delta function works for a number of different times"""
    td = TimeSeriesOps().get_time_delta("12:30")

    assert (td.seconds == 45000)

    td = TimeSeriesOps().get_time_delta("12:30:35")

    assert (td.seconds == 45035)

    print(td)

def test_overwrite_time_in_datetimeindex():
    """Tests that overwriting the time with a specific time of day works
    """
    # Clocks went forward in London on 00:00 31 Mar 2020
    datetimeindex = pd.date_range('28 Mar 2020', '05 Apr 2020', freq='h')
    datetimeindex = datetimeindex.tz_localize("utc")

    datetimeindex = TimeSeriesOps().overwrite_time_of_day_in_datetimeindex(datetimeindex, "16:00", overwrite_timezone="Europe/London")

    # Back in UTC time 16:00 LDN is 15:00 UTC after DST changes (and is 16:00 UTC beforehand)
    assert datetimeindex[0].hour == 16 and datetimeindex[-1].hour == 15

def test_chunk():
    """Tests the chunking of dataframes works
    """
    dt = pd.date_range(start='01 Jan 2018', end='05 Jan 2018', freq='1min')

    df = pd.DataFrame(index=dt, columns=['bid', 'mid', 'ask'])

    df['mid'] = np.random.random(len(dt))

    df_chunk = TimeSeriesOps().split_array_chunks(df, chunks=None, chunk_size=100)
    df_chunk = pd.concat(df_chunk)

    assert_frame_equal(df_chunk, df)

def test_cache_handle():
    """Tests the storing of DataFrames in the CacheHandle
    """
    from tcapy.data.volatilecache import VolatileRedis as VolatileCache
    volatile_cache = VolatileCache()

    dt = pd.date_range(start='01 Jan 2017', end='05 Jan 2019', freq='1m')
    df = pd.DataFrame(index=dt, columns=['bid', 'mid', 'ask'])

    df['mid'] = np.ones(len(dt))
    ch = volatile_cache.put_dataframe_handle(df, use_cache_handles=True)

    df_1 = volatile_cache.get_dataframe_handle(ch, burn_after_reading=True)

    assert_frame_equal(df, df_1)

def test_data_frame_holder():
    """Tests the storing of DataFrameHolder object which is like an enhanced dict specifically for storing DataFrames,
    alongside using the VolatileCache
    """
    from tcapy.analysis.dataframeholder import DataFrameHolder
    from tcapy.data.volatilecache import VolatileRedis as VolatileCache
    volatile_cache = VolatileCache()

    # Create a very large DataFrame, which needs to be chunked in storage
    dt = pd.date_range(start='01 Jan 2000', end='05 Jan 2020', freq='10s')
    df = pd.DataFrame(index=dt, columns=['bid', 'mid', 'ask'])

    df['bid'] = np.ones(len(dt))
    df['mid'] = np.ones(len(dt))
    df['ask'] = np.ones(len(dt))

    df_list = TimeSeriesOps().split_array_chunks(df, chunks=2)
    df_lower = df_list[0]
    df_higher = df_list[1]

    for i in ['_comp', '']:
        df_holder = DataFrameHolder()

        df_holder.add_dataframe(volatile_cache.put_dataframe_handle(df_lower, use_cache_handles=True), 'EURUSD_df' + i)
        df_holder.add_dataframe(volatile_cache.put_dataframe_handle(df_higher, use_cache_handles=True), 'EURUSD_df' + i)

        df_dict = df_holder.get_combined_dataframe_dict()

        df_final = df_dict['EURUSD_df' + i]

    assert_frame_equal(df, df_final)
