from __future__ import division, print_function

__author__ = 'saeedamen'  # Saeed Amen / saeed@cuemacro.com

#
# Copyright 2017 Cuemacro Ltd. - http//www.cuemacro.com / @cuemacro
#
# See the License for the specific language governing permissions and limitations under the License.
#

import time

from tcapy.conf.constants import Constants

constants = Constants()

from tcapy.analysis.tcaengine import TCAEngineImpl
from tcapy.analysis.tcarequest import TCARequest

from tcapy.analysis.algos.metric import *
from tcapy.analysis.algos.benchmark import *
from tcapy.analysis.algos.resultsform import *
from tcapy.analysis.algos.resultssummary import ResultsSummary

from tcapy.vis.report.tcareport import TCAReport
from tcapy.vis.tcaresults import TCAResults

from chartpy import Chart, Style

ticker = 'EURUSD'
mult_ticker = ['USDJPY', 'USDCAD']
start_date = '01 May 2017'; finish_date = '31 May 2017'

# 'dukascopy' or 'ncfx'
data_source = 'ncfx'

# Change the market and trade data store as necessary
market_data_store = 'arctic-' + data_source
trade_data_store = 'ms_sql_server'

tca_version = constants.tcapy_version

start_date = '05 Jan 2016'; finish_date = '31 May 2018'
#start_date = '01 Jan 2017'; finish_date = '31 May 2017'
start_date = '01 Jan 2018'; finish_date = '03 Jul 2018'
start_date = '15 Jun 2016'; finish_date = '10 Sep 2016' # Jul

long_start_date = '05 Jan 2016'; long_finish_date = '10 Feb 2016'

import os
print(os.environ['PATH'])

def single_ticker_tca_example():
    """Example for doing detailed TCA analysis on the trades of a single ticker, calculating metrics for slippage,
    transient market impact & permanent market impact. It also calculates benchmarks for arrival price of each trade and
    spread to mid).

    Creates a TCAReport which generates standalone HTML and PDF files

    Also on a lower level it collects results for slippage into a daily timeline and also average by venue (by default
    weights by reporting currency)
    """

    # Note: running Orca might not work in WSL, also when generating Plotly charts, might get an error with WSL, if
    # it doesn't have silent_display=True, as it will try to open a web page in a browser (which isn't supported in WSL1
    # but is in WSL2)
    PLOT = True

    # clear entire cache
    # Mediator.get_volatile_cache().clear_cache()

    tca_engine = TCAEngineImpl(version=tca_version)

    trade_order_type = 'trade_df'
    trade_order_list = ['trade_df', 'order_df']

    # Ensure orca is started, if want to convert to PDF (sometimes you may need to specify the path)
    # Can be slow to start
    if PLOT:
        from chartpy.engine import EnginePlotly
        EnginePlotly().start_orca()# constants.orca_server_path)

    # specify the TCA request
    tca_request = TCARequest(start_date=start_date, finish_date=finish_date, ticker=ticker,
                             tca_type='detailed',
                             dummy_market=False,
                             trade_data_store=trade_data_store, market_data_store=market_data_store,
                             metric_calcs=[# Calculate the slippage for trades/order
                                           MetricSlippage(trade_order_list=trade_order_list),

                                           # Calculate the shorter and longer term market impact after every trade/order
                                           MetricTransientMarketImpact(transient_market_impact_gap={'ms': 100},
                                                                       trade_order_list=trade_order_list),
                                           MetricPermanentMarketImpact(permanent_market_impact_gap={'h': 1},
                                                                       trade_order_list=trade_order_list)],
                             results_form=[# Aggregate the slippage average by date and hour
                                           TimelineResultsForm(metric_name='slippage', by_date='datehour', scalar=10000.0),

                                           # Aggregate the total executed notional in reporting currency (usually USD)
                                           # for every hour
                                           TimelineResultsForm(metric_name='executed_notional_in_reporting_currency',
                                                               by_date='datehour',
                                                               aggregation_metric='sum', scalar=1.0),

                                           # Aggregate the average slippage on trades by venue
                                           BarResultsForm(metric_name='slippage', aggregate_by_field='venue', scalar=10000.0,
                                                          trade_order_list='trade_df'),

                                           # Aggregate the average slippage on trades/orders by broker_id
                                           BarResultsForm(metric_name='slippage', aggregate_by_field='broker_id', scalar=10000.0),

                                           # Aggregate the average slippage on trades/orders by broker_id
                                           DistResultsForm(metric_name='slippage', aggregate_by_field='side', scalar=10000.0),

                                           # Create a scatter chart of slippage vs. executed notional
                                           ScatterResultsForm(scatter_fields=['slippage', 'executed_notional_in_reporting_currency'],
                                                              scalar={'slippage' : 10000.0})],
                             benchmark_calcs=[# At the arrival price for every trade/order
                                              BenchmarkArrival(),

                                              # At the spread at the time of every trade/order
                                              BenchmarkSpreadToMid()],
                             trade_order_mapping=trade_order_list, use_multithreading=False)

    # Dictionary of dataframes as output from TCA calculation
    dict_of_df = tca_engine.calculate_tca(tca_request)

    print(dict_of_df['trade_df'])

    print(dict_of_df.keys())

    timeline_slippage_df = dict_of_df['timeline_' + trade_order_type + '_slippage_by_all']
    timeline_executed_notional_df = dict_of_df['timeline_' + trade_order_type + '_executed_notional_in_reporting_currency_by_all'] # average slippage per day
    metric_df = dict_of_df[trade_order_type]['permanent_market_impact']  # permanent market impact for every trade

    print(metric_df.head(500))

    from tcapy.vis.report.computationreport import JinjaRenderer

    if PLOT:
        ### Generate TCA report using high level object
        # Use higher level TCAResults object to encapsulate results (easier to deal with than a dictionary of DataFrames)
        tca_results = TCAResults(dict_of_df, tca_request)
        tca_results.render_computation_charts()

        tca_report = TCAReport(tca_results, renderer=JinjaRenderer())

        tca_report.create_report(output_filename='test_tca_report.htm', output_format='html', offline_js=False)

        # Note needs plotly orca + wkhtmltopdf installed to render PDFs
        try:
            tca_report.create_report(output_filename='test_tca_report.pdf', output_format='pdf')
        except Exception as e:
            print(str(e))

        ### Lower level creation of TCA report

        ### Plot charts individually

        # Plot slippage by timeline
        Chart(engine='plotly').plot(timeline_slippage_df)

        # Plot total executed notional by timeline
        Chart(engine='plotly').plot(timeline_executed_notional_df)

        # Plot market impact (per trade)
        Chart(engine='plotly').plot(metric_df.head(500))

def simplest_tca_single_ticker_example():
    """Example for doing detailed TCA analysis on the trades of a single ticker, calculating metrics for slippage,
    transient market impact & permanent market impact. It also calculates benchmarks for arrival price of each trade and
    spread to mid).

    Collects results for slippage into a daily timeline and also average by venue (by default weights by reporting
    currency)
    """

    tca_engine = TCAEngineImpl(version=tca_version)

    # Specify the TCA request
    tca_request = TCARequest(start_date='01 Nov 2017', finish_date='20 Nov 2017', ticker='AUDUSD',
                             tca_type='detailed',
                             trade_data_store='ms_sql_server', market_data_store='arctic-ncfx',
                             metric_calcs=[MetricSlippage(trade_order_list=['trade_df', 'order_df'])],
                             results_form=[TimelineResultsForm(metric_name='slippage', by_date='date', scalar=10000.0)],
                             benchmark_calcs=[BenchmarkArrival(), BenchmarkSpreadToMid()],
                             trade_order_mapping=['trade_df', 'order_df'])

    # Dictionary of dataframes as output from TCA calculation
    dict_of_df = tca_engine.calculate_tca(tca_request)

    print(dict_of_df.keys())

    metric_df = dict_of_df['trade_df']['slippage']  # permanent market impact for every trade

    print(metric_df.head(500))

def multiple_ticker_tca_aggregated_example():
    """Example of how to do TCa analysis on multiple tickers
    """

    tca_engine = TCAEngineImpl(version=tca_version)

    # Run a TCA computation for multiple tickers, calculating slippage
    tca_request = TCARequest(start_date=start_date, finish_date=finish_date, ticker=mult_ticker, tca_type='aggregated',
                             trade_data_store=trade_data_store, market_data_store=market_data_store,
                             metric_calcs=MetricSlippage(), reporting_currency='EUR')

    dict_of_df = tca_engine.calculate_tca(tca_request)

    trade_df = dict_of_df['trade_df']

    # Aggregate some of the results with the ResultsSummary class (we could have done this within the TCARequest)
    summary = ResultsSummary()

    # Bucket slippage by ticker and report the average
    summary_slippage_df = summary.field_bucketing(trade_df, aggregate_by_field='ticker')

    print(summary_slippage_df)

    # Bucket slippage by ticker & return the average as weighted by the executed notional in reporting currency
    # (in this case EUR)
    summary_slippage_df = summary.field_bucketing(trade_df, aggregate_by_field='venue',
                                                  weighting_field='executed_notional_in_reporting_currency')

    print(summary_slippage_df)

    # Bucket slippage by ticker and report the average
    summary_slippage_df = summary.field_bucketing(trade_df, aggregate_by_field='venue')

    print(summary_slippage_df)

def venue_tca_aggregated_example():
    """Example of doing an aggregated TCA computation on a single ticker, and then later calculating the probability
    distribution function of slippage split by venue (when weighted by executed notional)
    """
    tca_engine = TCAEngineImpl(version=tca_version)

    tca_request = TCARequest(start_date=start_date, finish_date=finish_date, ticker=ticker, tca_type='aggregated',
                             trade_data_store=trade_data_store, market_data_store=market_data_store,
                             metric_calcs=MetricSlippage())

    dict_of_df = tca_engine.calculate_tca(tca_request)

    summary = ResultsSummary()

    summary_slippage_df = summary.field_distribution(dict_of_df['trade_df'], metric_name='slippage',
                                                     aggregate_by_field='venue', pdf_only=True,
                                                     weighting_field='executed_notional')

    # Plot PDF of slippage, split up by venue
    Chart(engine='plotly').plot(summary_slippage_df, style=Style(plotly_plot_mode='offline_html', connect_line_gaps=True))

def compare_multithreading_type():
    """Compares different type of multithreading types
    """
    tca_engine = TCAEngineImpl(version=tca_version)

    trade_order_list = ['trade_df', 'order_df']

    use_multithreading_list = [True, False]

    multithreading_params_list = [
        {'splice_request_by_dates' : True, 'cache_period' : 'day',
         'cache_period_trade_data' : True, 'cache_period_market_data' : True,
         'return_cache_handles_trade_data' : True, 'return_cache_handles_market_data' : True,
        'parallel_library' : 'celery'},
        {'splice_request_by_dates': False, 'cache_period': 'day',
         'cache_period_trade_data': True, 'cache_period_market_data': True,
         'return_cache_handles_trade_data' : True, 'return_cache_handles_market_data' : True,
         'parallel_library': 'celery'}
        ]

    for use_multithreading in use_multithreading_list:
        for multithreading_params in multithreading_params_list:
            start = time.time()

            # Specify the TCA request
            tca_request = TCARequest(start_date=long_start_date, finish_date=long_finish_date, ticker=ticker,
                                     tca_type='detailed',
                                     trade_data_store=trade_data_store, market_data_store=market_data_store,
                                     metric_calcs=[MetricSlippage(trade_order_list=trade_order_list),
                                                   MetricTransientMarketImpact(transient_market_impact_gap={'ms': 100},
                                                                               trade_order_list=trade_order_list),
                                                   MetricPermanentMarketImpact(permanent_market_impact_gap={'h': 1},
                                                                               trade_order_list=trade_order_list)],
                                     results_form=[TimelineResultsForm(metric_name='slippage', by_date='datehour', scalar=10000.0)],
                                     benchmark_calcs=[BenchmarkArrival(), BenchmarkSpreadToMid()],
                                     trade_order_mapping=trade_order_list, use_multithreading=use_multithreading,
                                     multithreading_params=multithreading_params)

            # Dictionary of dataframes as output from TCA calculation
            dict_of_df = tca_engine.calculate_tca(tca_request)

            finish = time.time()

            print('Multithreading example: calculated ' + str(round(finish - start, 3)) + "s for, use_multithreading = "
                  + str(use_multithreading) + ' multithreading_params = ' + str(multithreading_params))


if __name__ == '__main__':
    start = time.time()

    Mediator.get_volatile_cache().clear_cache()

    single_ticker_tca_example()
    simplest_tca_single_ticker_example()
    multiple_ticker_tca_aggregated_example()
    venue_tca_aggregated_example()
    compare_multithreading_type()

    finish = time.time()
    print('Status: calculated ' + str(round(finish - start, 3)) + "s")
