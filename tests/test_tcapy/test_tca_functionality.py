"""Tests out the bulk of the TCA calculations including metric calculations (like slippage), calculating orders
executed prices from executions, filtering of trades, calculations of benchmarks (like TWAP, arrival price etc.) and so on
"""

from __future__ import division, print_function

__author__ = 'saeedamen'  # Saeed Amen / saeed@cuemacro.com

#
# Copyright 2017 Cuemacro Ltd. - http//www.cuemacro.com / @cuemacro
#
# See the License for the specific language governing permissions and limitations under the License.
#

from pandas.testing import assert_frame_equal
import pandas as pd
import numpy as np
import os

from tcapy.util.fxconv import FXConv

from tcapy.analysis.tcaengine import TCAEngineImpl

from tcapy.analysis.tcarequest import TCARequest, MarketRequest

from tcapy.analysis.algos.benchmark import *
from tcapy.analysis.algos.metric import MetricSlippage, MetricTransientMarketImpact
from tcapy.analysis.algos.resultsform import *
from tcapy.analysis.tradeorderfilter import *
from tcapy.vis.tcaresults import TCAResults
from tcapy.vis.report.tcareport import TCAReport

from collections import OrderedDict

from tests.config import resource

constants = Constants()
logger = LoggerManager().getLogger(__name__)

from tcapy.util.mediator import Mediator

tcapy_version = constants.tcapy_version

logger.info('Make sure you have created folder ' + constants.csv_folder + ' & ' + constants.temp_data_folder +
            ' otherwise tests will fail')

########################################################################################################################
# YOU MAY NEED TO CHANGE TESTING PARAMETERS IF YOUR DATABASE DOESN'T COVER THESE DATES
start_date = '01 May 2017'
finish_date = '30 May 2017'

filter_date = '03 May 2017'
start_filter_date = '00:00:00 03 May 2017'
finish_filter_date = '23:59:59 03 May 2017'

# Current data vendors are 'ncfx' or 'dukascopy'
data_source = 'dukascopy'
trade_data_store = 'ms_sql_server'
market_data_store = 'arctic-' + data_source

ticker = 'EURUSD'
reporting_currency = 'USD'
tca_type = 'aggregated'
venue_filter = 'venue1'

# Mainly just to speed up tests - note: you will need to generate the HDF5 files using convert_csv_to_h5.py from the CSVs
use_hdf5_market_files = False

missing_ticker = 'AUDSEK'

# So we are not specifically testing the database of tcapy - can instead use CSV in the test harness folder
use_trade_test_csv = True
use_market_test_csv = False

########################################################################################################################

# you can change the test_data_harness_folder to one on your own machine with real data
folder = constants.test_data_harness_folder

eps = 10 ** -5

trade_order_mapping = constants.test_trade_order_list
trade_df_name = trade_order_mapping[0] # usually 'trade_df'
order_df_name = trade_order_mapping[1] # usually 'order_df'

if use_market_test_csv:

    # Only contains limited amount of EURUSD and USDJPY in Apr/Jun 2017
    if use_hdf5_market_files:
        market_data_store = os.path.join(folder, 'small_test_market_df.h5')
    else:
        market_data_store = os.path.join(folder, 'small_test_market_df.csv.gz')

if use_trade_test_csv:
    trade_data_store = 'csv'

    trade_order_mapping = OrderedDict([(trade_df_name, resource('small_test_trade_df.csv')),
                                       (order_df_name, resource('small_test_order_df.csv'))])
    venue_filter = 'venue1'
else:
    # Define your own trade order mapping
    pass

def get_sample_data(ticker_spec=None):
    if ticker_spec is None: ticker_spec = ticker

    logger.info("About to load data for " + ticker_spec)

    tca_request = TCARequest(start_date=start_date, finish_date=finish_date, ticker=ticker_spec,
                             trade_data_store=trade_data_store,
                             reporting_currency=reporting_currency,
                             market_data_store=market_data_store,
                             trade_order_mapping=trade_order_mapping,
                             tca_type=tca_type, benchmark_calcs=BenchmarkMarketMid())

    tca_engine = TCAEngineImpl(version=tcapy_version)

    trade_order_results_df_dict = tca_engine.calculate_tca(tca_request)

    return trade_order_results_df_dict[ticker_spec + "_df"], trade_order_results_df_dict[trade_df_name], \
           trade_order_results_df_dict[order_df_name]


def test_full_detailed_tca_calculation():
    """Tests a detailed TCA calculation, checking that it has the right tables returned.
    """

    tca_request = TCARequest(start_date=start_date, finish_date=finish_date, ticker=ticker,
                             trade_data_store=trade_data_store,
                             market_data_store=market_data_store,
                             trade_order_mapping=trade_order_mapping)

    tca_engine = TCAEngineImpl(version=tcapy_version)

    dict_of_df = tca_engine.calculate_tca(tca_request=tca_request)

    assert (trade_df_name in dict_of_df and 'sparse_market_' + trade_df_name in dict_of_df and 'market_df' in dict_of_df)

    tca_request.ticker = missing_ticker

    data_missing_exception = False

    try:
        dict_of_df = tca_engine.calculate_tca(tca_request=tca_request)
    except DataMissingException:
        data_missing_exception = True

    assert data_missing_exception

def test_invalid_tca_inputs():
    """Check exception is thrown with TCAEngine if ticker is not valid (eg. if none, or just a random string of 6 letters,
    or if the includes '/'
    """

    tca_engine = TCAEngineImpl(version=tcapy_version)

    invalid_tickers = [None, 'KRPAZY', 'EUR/USD']

    for t in invalid_tickers:
        tca_request = TCARequest(start_date=start_date, finish_date=finish_date, ticker=t)

        ticker_exception_ok = []

        try:
            trade_order_results_df_dict = tca_engine.calculate_tca(tca_request)

            ticker_exception_ok.append(False)
        except Exception as e:
            if isinstance(e, ValidationException):
                ticker_exception_ok.append(True)

    ### Check exception is thrown with TCAEngine if start/finish dates are messed up
    date_exception_ok = []

    try:
        tca_request = TCARequest(start_date='01 Mar19', finish_date='01Oc t20', ticker='EURUSD')

        date_exception_ok.append(False)
    except Exception as e:
        if isinstance(e, DateException):
            date_exception_ok.append(True)

    assert any(ticker_exception_ok) and any(date_exception_ok)

def test_metric_calculation():
    """Tests slippage calculation on a test set of market and trade data
    """

    market_df, trade_df, order_df = get_sample_data()

    #### Calculate slippage
    market_df.index = market_df.index + timedelta(milliseconds=5)

    # Add a mid point (in case it doesn't exist)
    _, market_df = BenchmarkMarketSpreadToMid().calculate_benchmark(market_df=market_df)

    trade_df, _ = MetricSlippage().calculate_metric(trade_order_df=trade_df, market_df=market_df, bid_benchmark='mid',
                                                    ask_benchmark='mid')

    # a selection of points to try
    ind_list = [0, 1, 2, -2 -1]

    for i in ind_list:
        # now replicate slippage calculation from first principles (get the last available point if no match)
        mid_index = market_df['mid'].index.get_loc(trade_df.index[i], method='ffill')

        trade = trade_df['executed_price'][i]

        side = trade_df['side'][i]

        slippage = trade_df['slippage'][i]
        market_slippage = trade_df['slippage_benchmark'][i]

        market = market_df.ix[mid_index]['mid']

        # Do slippage calculation for comparison with our method
        slippage_comp = -side * (trade - market)

        # Check that the 'slippage' column exists and is consistent
        assert ('slippage' in trade_df.columns and abs(slippage - slippage_comp) < eps)

    ### check anomalous trade identification
    market_df, trade_df, order_df = get_sample_data()

    market_df.index = market_df.index + timedelta(milliseconds=10)

    # Force spread to mid to be 0.25bp
    anomalous_spread_to_mid_bp = 0.25

    _, market_df = BenchmarkMarketSpreadToMid().calculate_benchmark(trade_order_df=trade_df, market_df=market_df,
                                                                    bid_mid_bp=anomalous_spread_to_mid_bp,
                                                                    ask_mid_bp=anomalous_spread_to_mid_bp, overwrite_bid_ask=True)

    trade_df, _ = MetricSlippage().calculate_metric(trade_order_df=trade_df, market_df=market_df)

    anomalous_metric = trade_df[trade_df['slippage_anomalous'] == 1]
    anomalous_comparison = trade_df[trade_df['slippage'] <= -(anomalous_spread_to_mid_bp / (100.0 * 100.0))]

    # Now test if the correct trades have been identified as anomalous
    assert_frame_equal(anomalous_metric[['slippage_anomalous']], anomalous_comparison[['slippage_anomalous']])

    #### Calculate market impact (using bid/ask)
    market_df, trade_df, order_df = get_sample_data()

    trade_df, _ = MetricTransientMarketImpact(transient_market_impact_gap={'ms' : 1250}).calculate_metric(
        trade_order_df=trade_df, market_df=market_df, bid_benchmark='bid', ask_benchmark='ask')

    for i in ind_list:
        # Now replicate transient market impact calculation from first principles (get the NEXT available point if not available)
        time_to_search = trade_df.index[i] + timedelta(milliseconds=1250)

        index = market_df.index.get_loc(time_to_search, method='bfill')
        index_time = market_df.index[index]

        trade = trade_df['executed_price'][i]

        side = trade_df['side'][i]

        if 'bid' in market_df.columns and 'ask' in market_df.columns:
            if side == 1:
                market = market_df.ix[index]['ask']
            elif side == -1:
                market = market_df.ix[index]['bid']
        else:
            market = market_df.ix[index]['mid']

        market_transient_impact_benchmark = trade_df['transient_market_impact_benchmark'][i]

        transient_market_impact = trade_df['transient_market_impact'][i]

        # do transient market impact calculation for comparison with our method
        transient_market_impact_comp = side * (trade - market)

        # check that the 'transient_market_impact' column exists and is consistent
        assert ('transient_market_impact' in trade_df.columns) and \
               (abs(transient_market_impact - transient_market_impact_comp) < eps)

def test_benchmark_calculation():
    """Tests TWAP/VWAP/Arrival benchmark calculation for an order (between the order's start and finish)
    """

    market_df, trade_df, order_df = get_sample_data()

    # In case there's no volume data, we can make it up for testing purposes!
    market_df['volume'] = market_df['mid'] * 0.9

    #### TWAP calculation
    order_df, _ = BenchmarkTWAP().calculate_benchmark(trade_order_df=order_df, market_df=market_df)

    ind_list = [0, 1, 2, -2 - 1]

    for i in ind_list:
        if order_df.ix[i, 'notional'] > 1:
            twap_price = order_df.ix[i, 'twap']

            market_prices = market_df[order_df.iloc[i]['benchmark_date_start']:order_df.iloc[i]['benchmark_date_end']]['mid']

            dt = market_prices.index.tz_convert(None).to_series().diff().values / np.timedelta64(1, 's')
            dt[0] = 0

            twap_price_comparison = (market_prices * dt).sum() / dt.sum()

            assert abs(twap_price - twap_price_comparison) < eps

    #### VWAP calculation
    order_df, _ = BenchmarkVWAP().calculate_benchmark(trade_order_df=order_df, market_df=market_df, weighting_field='volume')

    for i in ind_list:
        if order_df.ix[i, 'notional'] > 1:
            vwap_price = order_df.ix[i, 'vwap']

            market_prices = market_df[order_df.iloc[i]['benchmark_date_start']:order_df.iloc[i]['benchmark_date_end']]['mid']

            volume = market_df[order_df.iloc[i]['benchmark_date_start']:order_df.iloc[i]['benchmark_date_end']]['volume']

            vwap_price_comparison = (market_prices * volume).sum() / volume.sum()

            assert abs(vwap_price - vwap_price_comparison) < eps

    #### Arrival calculation
    order_df, _ = BenchmarkArrival().calculate_benchmark(trade_order_df=order_df, market_df=market_df)

    for i in ind_list:
        if order_df.ix[i, 'notional'] > 1:
            arrival_price = order_df.ix[i, 'arrival']

            # the arrival calculation uses a different implementation which is vectorizable (unlike get_loc)
            approx = market_df.index.get_loc(order_df.index[i], method='pad')

            arrival_price_comparison = market_df.iloc[approx]['mid']

            assert abs(arrival_price - arrival_price_comparison) < eps

def test_tag_filter_calculation():
    """Test we can filter by venue and by broker correctly.
    """

    trade_order_filter = TradeOrderFilterTag(tag_value_combinations={'broker_id': 'broker1'})

    tca_request = TCARequest(start_date=start_date, finish_date=finish_date, ticker=ticker,
                             trade_data_store=trade_data_store,
                             reporting_currency=reporting_currency,
                             market_data_store=market_data_store,
                             trade_order_mapping=trade_order_mapping,
                             tca_type=tca_type,
                             trade_order_filter=trade_order_filter,
                             venue='venue1')

    tca_engine = TCAEngineImpl(version=tcapy_version)

    trade_order_results_df_dict = tca_engine.calculate_tca(tca_request)

    trade_df = trade_order_results_df_dict[trade_df_name]

    if trade_df is not None:
        if not(trade_df.empty):

            # note that this only works with the "test" data - it won't work with real data!
            match_brokers = len(trade_df[trade_df['broker_id'] == 'broker1'])
            non_brokers = len(trade_df[trade_df['broker_id'] != 'broker1'])

            match_venue = len(trade_df[trade_df['venue'] == 'venue1'])
            non_match_venue = len(trade_df[trade_df['venue'] != 'venue1'])

            # check the filtering has been correctly, so we only have trades by broker1 and venue1
            assert match_brokers > 0 and non_brokers == 0 and match_venue > 0 and non_match_venue == 0

def test_time_of_day_filter_calculation():
    """Test we can filter by time of day/date
    """

    trade_order_filter = TradeOrderFilterTimeOfDayWeekMonth(specific_dates=filter_date)

    tca_request = TCARequest(start_date=start_date, finish_date=finish_date, ticker=ticker,
                             trade_data_store=trade_data_store,
                             reporting_currency=reporting_currency,
                             market_data_store=market_data_store,
                             trade_order_mapping=trade_order_mapping,
                             tca_type=tca_type,
                             trade_order_filter=trade_order_filter)

    tca_engine = TCAEngineImpl(version=tcapy_version)

    trade_order_results_df_dict = tca_engine.calculate_tca(tca_request)

    trade_df = trade_order_results_df_dict[trade_df_name]

    if trade_df is not None:
        if not(trade_df.empty):

            match_filtered_date = len(trade_df[start_filter_date:finish_filter_date])
            non_filtered_date = len(trade_df[(trade_df.index > finish_filter_date) & (trade_df.index < start_filter_date)])

            # check the filtering has been correctly, so we only have trades by broker1 and venue1
            assert match_filtered_date > 0 and non_filtered_date == 0

def test_executed_price_notional_calculation():
    """Test that the executed average price calculation from trades is correctly reflected in the order level
    """
    Mediator.get_volatile_cache().clear_cache()

    market_df, trade_df, order_df = get_sample_data()

    # get the first and last points given boundary cases (and a few other random orders) to check
    index_boundary = np.random.randint(0, len(order_df.index) - 1, 100)
    index_boundary = index_boundary.tolist();
    index_boundary.append(0);
    index_boundary.append(-1)

    for i in index_boundary:
        if order_df.ix[i, 'notional'] > 1:
            executed_price = order_df.ix[i, 'executed_price']
            id = order_df.ix[i, 'id']

            executed_price_trade = trade_df[trade_df['ancestor_pointer_id'] == id]['executed_price'].fillna(0)
            executed_notional_trade = trade_df[trade_df['ancestor_pointer_id'] == id]['executed_notional'].fillna(0)

            executed_avg_trade = ((executed_price_trade * executed_notional_trade).sum() / executed_notional_trade.sum())

            assert abs(executed_price - executed_avg_trade) < eps

def test_data_offset():
    """Tests the offsetting of market and trade data by milliseconds by user. This might be useful if clocks are slightly
    offset when recording market or trade data
    """
    Mediator.get_volatile_cache().clear_cache()

    tca_request = TCARequest(start_date=start_date, finish_date=finish_date, ticker=ticker,
                             trade_data_store=trade_data_store,
                             market_data_store=market_data_store,
                             trade_order_mapping=trade_order_mapping)

    tca_engine = TCAEngineImpl(version=tcapy_version)

    dict_of_df = tca_engine.calculate_tca(tca_request=tca_request)

    # Now offset both the trade and market data
    tca_request.trade_data_offset_ms = 1
    tca_request.market_data_offset_ms = -1

    dict_of_df_offset = tca_engine.calculate_tca(tca_request=tca_request)

    trade_df = dict_of_df[trade_df_name]; market_df = dict_of_df['market_df']
    trade_df_offset = dict_of_df_offset[trade_df_name]; market_df_offset = dict_of_df_offset['market_df']

    assert all(market_df.index + timedelta(milliseconds=-1) == market_df_offset.index)
    assert all(trade_df.index + timedelta(milliseconds=1) == trade_df_offset.index)

    for c in constants.date_columns:
        if c in trade_df.columns:
            assert all(trade_df[c]+ timedelta(milliseconds=1) == trade_df_offset[c])


def test_market_data_convention():
    """Tests that market data for unusual quotations is consistent (ie. if the user requests USDEUR, this should be
    inverted EURUSD (which is the correct convention)
    """
    market_loader =  Mediator.get_tca_market_trade_loader(version=tcapy_version)
    market_request = MarketRequest(start_date=start_date, finish_date=finish_date, ticker=ticker,
                             data_store=market_data_store)

    #### Compare EURUSD to USDEUR
    market_correct_conv_series = pd.DataFrame(market_loader.get_market_data(market_request)['mid'])

    market_request.ticker = 'USDEUR'
    market_reverse_conv_series = pd.DataFrame(1.0 / market_loader.get_market_data(market_request)['mid'])

    assert_frame_equal(market_correct_conv_series, market_reverse_conv_series, check_dtype=False)

    ### Compare EURJPY (which is autogenerated, if EURJPY is not collected directly) vs. EURUSD & USDJPY multiplied

    # Use resampled series for comparison
    market_request.ticker = 'USDJPY'
    market_df_USDJPY = pd.DataFrame(market_loader.get_market_data(market_request)['mid'])

    market_request.ticker = 'EURJPY'
    market_df_EURJPY = pd.DataFrame(market_loader.get_market_data(market_request)['mid']).resample('1min').mean()

    market_df_EURJPY_comp = (market_correct_conv_series.resample('1min').mean() * market_df_USDJPY.resample('1min').mean())

    market_df_EURJPY, market_df_EURJPY_comp = market_df_EURJPY.align(market_df_EURJPY_comp, join='inner')

    comp = (market_df_EURJPY - market_df_EURJPY_comp).dropna()

    assert all(comp < eps)

def test_quotation_conv():
    """Check that we can correctly classify if an FX cross is in the right convention
    """
    FX_pair = "EURUSD"
    incorrect_FX_pair = "JPYAUD"

    fx_conv = FXConv()

    assert fx_conv.is_EM_cross(FX_pair) == False and fx_conv.correct_notation(incorrect_FX_pair) == 'AUDJPY'

def test_results_form_average():
    """Tests averages are calculated correctly by ResultsForm, compared to a direct calculation
    """

    market_df, trade_df, order_df = get_sample_data()

    trade_df, _ = MetricSlippage().calculate_metric(trade_order_df=trade_df, market_df=market_df, bid_benchmark='mid',
                                                    ask_benchmark='mid')

    results_form = BarResultsForm(trade_order_list=[trade_df_name],
                                       metric_name='slippage',
                                       aggregation_metric='mean',
                                       aggregate_by_field=['ticker', 'venue'], scalar=10000.0,
                                       weighting_field='executed_notional_in_reporting_currency')

    results_df = results_form.aggregate_results(trade_order_df=trade_df, market_df=market_df, trade_order_name=trade_df_name)

    slippage_average = float(results_df[0][0].values[0])

    # Directly calculate slippage
    def grab_slippage(trade_df):
        return 10000.0 * ((trade_df['slippage'] * trade_df['executed_notional_in_reporting_currency']).sum() \
                   / trade_df['executed_notional_in_reporting_currency'].sum())

    slippage_average_comp = grab_slippage(trade_df)

    # Check the average slippage
    assert slippage_average - slippage_average_comp < eps

    slippage_average_venue = results_df[1][0]['venue'][venue_filter]

    slippage_average_venue_comp = grab_slippage(trade_df[trade_df['venue'] == venue_filter])

    # Check the average slippage by venue
    assert slippage_average_venue - slippage_average_venue_comp < eps

def test_create_tca_report():
    """Tests the creation of a TCAResults, checking they are fichecking it generates the right document
    """

    tca_request = TCARequest(start_date=start_date, finish_date=finish_date, ticker=ticker,
                             trade_data_store=trade_data_store,
                             market_data_store=market_data_store,
                             trade_order_mapping=trade_order_mapping,
                             metric_calcs=MetricSlippage(),
                             results_form=TimelineResultsForm(metric_name='slippage', by_date='datehour'))

    tca_engine = TCAEngineImpl(version=tcapy_version)

    tca_results = TCAResults(tca_engine.calculate_tca(tca_request=tca_request), tca_request)
    tca_results.render_computation_charts()

    assert tca_results.timeline is not None and tca_results.timeline_charts is not None

    tca_report = TCAReport(tca_results)
    html = tca_report.create_report()

    # Quick check to see that the html has been generated by checking existance of HTML head tag
    assert '<head>' in html
