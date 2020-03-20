"""Tests more involved functionality of TCA such as multithreading (and checks computations are
the same as single threaded - note that if there are gaps in market data, there could be differences) and also if there
are multiple requests made at same time (stress testing).

Note for these tests to work we need to have properly populated databases of market/trade/order data.

Alternatively, we can use CSV files for the market and trade/order data in this test, although this will diminish
the usefulness of the stress test.
"""

from __future__ import division, print_function

__author__ = 'saeedamen'  # Saeed Amen / saeed@cuemacro.com

#
# Copyright 2017 Cuemacro Ltd. - http//www.cuemacro.com / @cuemacro
#
# See the License for the specific language governing permissions and limitations under the License.
#

try:
    from pandas.testing import assert_frame_equal
except:
    from pandas.util.testing import assert_frame_equal

import pandas as pd
import os

from tcapy.util.customexceptions import *
from tcapy.conf.constants import Constants

from tcapy.util.mediator import Mediator

constants = Constants()
tcapy_version = constants.tcapy_version

from tcapy.analysis.tcaengine import TCARequest, TCAEngineImpl

########################################################################################################################
# YOU MAY NEED TO CHANGE TESTING PARAMETERS IF YOUR DATABASE DOESN'T COVER THESE DATES
start_date = '02 May 2017';
finish_date = '05 Jun 2017'

# Can try longer time periods - will make tests slower, but will make tests more exhaustive
# start_date = '01 Jan 2016'; finish_date = '01 Mar 2018'

# Stress test for TCA analysis (larger dataset more likely to see any issues)
# multithreading_start_date = '01 May 2017'; multithreading_finish_date = '20 Jul 2017'
multithreading_start_date = '01 May 2017'; multithreading_finish_date = '02 Jul 2017'
large_start_date = '01 Jan 2016';
large_finish_date = '01 Apr 2016'

market_data_store = 'arctic-ncfx'
trade_data_store = 'ms_sql_server'
ticker = 'EURUSD'

valid_ticker_list = ['EURUSD', 'GBPUSD', 'AUDSEK']  # it should return gracefully if at least 1 ticker exists
missing_ticker = 'AUDSEK'

from collections import OrderedDict

trade_order_mapping = ['trade_df', 'order_df']

# start_date = '01 Oct 2017'; finish_date = '15 Nov 2017'; ticker = 'GBPUSD'

invalid_start_date = '02 May 2000';
invalid_finish_date = '05 May 2001'

# So we are not specifically testing the database of tcapy - can instead use CSV in the test harness folder
use_trade_test_csv = False
use_market_test_csv = False

dump_csv_output = True  # Can slow down testing, but is useful for debugging purposes

# Do a stress test, calling several large TCA requests in quick succession
stress_test = True

########################################################################################################################

# you can change the test_data_harness_folder to one on your own machine with real data
folder = constants.test_data_harness_folder

trade_order_mapping = constants.test_trade_order_list
trade_df_name = trade_order_mapping[0] # usually 'trade_df'
order_df_name = trade_order_mapping[1] # usually 'order_df'

eps = 10 ** -3

if use_market_test_csv:
    # only contains limited amount of EURUSD and USDJPY in Apr/Jun 2017
    market_data_store = os.path.join(folder, 'small_test_market_df.csv.gz')

if use_trade_test_csv:
    trade_data_store = 'csv'

    trade_order_mapping = OrderedDict([(trade_df_name, os.path.join(folder, 'small_test_trade_df.csv')),
                                       (order_df_name, os.path.join(folder, 'small_test_order_df.csv'))])

else:
    # define your own trade order mapping
    pass

# Test will only succeed if the gen specific version is set
assert constants.tcapy_version == 'gen'

def test_multithreading_full_basic_tca():
    """Tests if the trade/order and market data is identical for multithreading versus singlethreading for detailed,
    aggregated and compliance. Note that we need a running Celery server for multithreading to work (as well as the
    usual SQL and Arctic databases running, if the test_csv option has not been selected). Uses a very large data sample
    """
    Mediator.get_volatile_cache(version=tcapy_version).clear_cache()  # clear cache to ensure all test code runs!

    tca_request = TCARequest(start_date=multithreading_start_date, finish_date=multithreading_finish_date,
                             ticker=valid_ticker_list,
                             trade_data_store=trade_data_store,
                             market_data_store=market_data_store,
                             trade_order_mapping=trade_order_mapping)

    tca_engine = TCAEngineImpl(version=tcapy_version)

    tca_type = ['aggregated', 'detailed', 'compliance']

    multithreading = [True, False]

    #### Checked the executed prices match with single and multithreaded cases
    for t in tca_type:
        dict_list = []

        for m in multithreading:
            Mediator.get_volatile_cache(
                version=tcapy_version).clear_cache()  # Clear cache to ensure all test code runs!

            tca_request.use_multithreading = m
            tca_request.tca_type = t
            dict_list.append(tca_engine.calculate_tca(tca_request=tca_request))

        print("tca_request " + t)

        for k in dict_list[0].keys():
            multi_df = dict_list[0][k]
            single_df = dict_list[1][k]

            if isinstance(single_df, pd.DataFrame) and isinstance(multi_df, pd.DataFrame):
                if 'executed_price' in single_df.columns and 'executed_price' in multi_df.columns:

                    print("tablecomputation " + k)

                    exec_multi = multi_df['executed_price'].dropna()
                    exec_single = single_df['executed_price'].dropna()

                    if dump_csv_output:
                        df = pd.DataFrame(exec_multi)

                        df1 = pd.DataFrame(exec_single)
                        df1.columns = [x + '_single' for x in df1.columns]

                        df = df.join(pd.DataFrame(df1), how='outer')

                        df.to_csv(k + "_test.csv")

                        df_large = single_df.join(multi_df, lsuffix='_single', rsuffix='_multi', how='outer')
                        df_large.to_csv(k + "_test_full.csv")

                    assert all(exec_multi - exec_single < eps)

                    # Only check trade/orders and not any of the other DataFrames returned
                    if 'id' in multi_df.columns and 'id' in single_df.columns:
                        # check we have unique IDs (we should have unique IDs for every event_type trade (but the id's will
                        # be the same for placements)
                        id_multi = multi_df['id']
                        id_single = single_df['id']

                        assert len(id_multi.index) == len(id_multi.index)

                        assert len(id_multi.unique()) == len(id_single.index)
                        assert len(id_multi.index) == len(id_single.unique())


def test_invalid_dates_missing_data_tca():
    """Tests if the trade/order and market data is identical for multithreading versus singlethreading for detailed,
    aggregated and compliance. Note that we need a running Celery server for multithreading to work (as well as the
    usual SQL and Arctic databases running, if the test_csv option has not been selected). Uses a very large data sample
    """
    Mediator.get_volatile_cache(version=tcapy_version).clear_cache()  # clear cache to ensure all test code runs!

    tca_request = TCARequest(start_date=large_start_date, finish_date=large_finish_date, ticker=valid_ticker_list,
                             trade_data_store=trade_data_store,
                             market_data_store=market_data_store,
                             trade_order_mapping=trade_order_mapping)

    tca_engine = TCAEngineImpl(version=tcapy_version)

    tca_type = ['detailed', 'aggregated', 'compliance']

    multithreading = [True, False]

    ## test invalid dates
    tca_request.start_date = invalid_start_date;
    tca_request.finish_date = invalid_finish_date

    for t in tca_type:
        for m in multithreading:
            Mediator.get_volatile_cache(
                version=tcapy_version).clear_cache()  # clear cache to ensure all test code runs!
            tca_request.use_multithreading = m
            tca_request.tca_type = t

            exception_triggered = True

            try:
                dict_of_df_invalid = tca_engine.calculate_tca(tca_request=tca_request)

                exception_triggered = False

            except DataMissingException:
                assert exception_triggered

    ## test a single valid ticker, but missing data (only one ticker)
    tca_request.start_date = start_date;
    tca_request.finish_date = finish_date;
    tca_request.ticker = missing_ticker

    for t in tca_type:
        for m in multithreading:
            Mediator.get_volatile_cache(
                version=tcapy_version).clear_cache()  # clear cache to ensure all test code runs!
            tca_request.use_multithreading = m
            tca_request.tca_type = t

            exception_triggered = True

            try:
                dict_of_df_missing_ticker = tca_engine.calculate_tca(tca_request=tca_request)

                exception_triggered = False

            except DataMissingException:
                assert exception_triggered

def test_stress_tca():
    """Makes several large TCARequests at the same time to stress test tcapy application and also to check it works
    with paralle requests (note: you may need to reduce the length of the dataset if your machine has limited amounts of RAM).

    It can be possible that when deployed on the web, several users might make simultaneous requests. Note, do not use
    pylibmc, and instead use python-memcached, when using memcached as a result backend. pylibmc is not thread-safe so
    will come undone if you end up making parallel requests.
    """
    from tcapy.util.swim import Swim

    if not (stress_test):
        return

    # Clear cache to ensure all test code runs!
    Mediator.get_volatile_cache(version=tcapy_version).clear_cache()

    tca_request = TCARequest(start_date=large_start_date, finish_date=large_finish_date, ticker=['All'],
                             dummy_market=True,
                             trade_data_store=trade_data_store,
                             market_data_store=market_data_store,
                             trade_order_mapping=trade_order_mapping, use_multithreading=True, tca_type='aggregated')

    # Kick off several simulanteous large TCA requests
    request_no = 2

    tca_request_list = []

    for i in range(0, request_no):
        tca_request_list.append(TCARequest(tca_request=tca_request))

    tca_engine = TCAEngineImpl(version=tcapy_version)

    swim = Swim(parallel_library='thread')
    pool = swim.create_pool(thread_no=len(tca_request_list))

    result = []

    for i in range(0, len(tca_request_list)):
        result.append(
            pool.apply_async(tca_engine.calculate_tca,
                             args=(tca_request_list[i],)))

    output = [p.get() for p in result]

    swim.close_pool(pool, True)

    assert len(output) == len(tca_request_list)

    # Check that several DataFrames exist in the results
    for trade_order_results_df_dict in output:
        assert 'trade_df' in trade_order_results_df_dict.keys()

if __name__ == '__main__':
    test_multithreading_full_basic_tca()
    test_invalid_dates_missing_data_tca()

    test_stress_tca()

    # import pytest; pytest.main()
