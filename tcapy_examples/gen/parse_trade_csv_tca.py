from __future__ import division, print_function

__author__ = 'saeedamen'  # Saeed Amen / saeed@cuemacro.com

#
# Copyright 2017 Cuemacro Ltd. - http//www.cuemacro.com / @cuemacro
#
# See the License for the specific language governing permissions and limitations under the License.
#

import os

from collections import OrderedDict

from tcapy.conf.constants import Constants

from tcapy.analysis.tcaengine import TCAEngineImpl
from tcapy.analysis.tcarequest import TCARequest

from tcapy.analysis.algos.metric import *
from tcapy.analysis.algos.benchmark import *
from tcapy.analysis.algos.resultsform import *

from tcapy.data.databasesource import DatabaseSourceCSV
from tcapy.util.fxconv import FXConv

constants = Constants()

folder = constants.test_data_harness_folder

from chartpy import Style

# 'dukascopy' or 'ncfx'
data_source = 'ncfx'

# Change the market and trade data store as necessary
market_data_store = 'arctic-' + data_source
trade_data_store = 'dataframe'

csv_trade_order_mapping = OrderedDict([('trade_df', os.path.join(folder, 'small_test_trade_df.csv')),
                                       ('order_df', os.path.join(folder, 'small_test_order_df.csv'))])

tca_version = constants.tcapy_version

def dataframe_tca_example():
    """Example for doing detailed TCA analysis on all the trades in a CSV, calculating metrics for slippage,
    transient market impact & permanent market impact. It also calculates benchmarks for arrival price of each trade and
    spread to mid).

    Collects results for slippage into a daily timeline and also average by venue (by default weights by reporting
    currency)
    """
    PLOT = False

    # clear entire cache
    # Mediator.get_volatile_cache(version='pro').clear_cache()

    tca_engine = TCAEngineImpl(version=tca_version)

    trade_order_type = 'trade_df'
    trade_order_list = ['trade_df']

    trade_df = DatabaseSourceCSV(trade_data_database_csv=csv_trade_order_mapping['trade_df']).fetch_trade_order_data()

    data_frame_trade_order_mapping = OrderedDict([('trade_df', trade_df)])

    start_date = trade_df.index[0]; finish_date = trade_df.index[-1]

    ticker_list = FXConv().correct_unique_notation_list(trade_df['ticker'].unique().tolist())

    # Specify the TCA request
    tca_request = TCARequest(start_date=start_date, finish_date=finish_date, ticker=ticker_list,
                             tca_type='aggregated', dummy_market=True,
                             trade_data_store='dataframe', market_data_store=market_data_store,
                             metric_calcs=[MetricSlippage(trade_order_list=trade_order_list),
                                           MetricTransientMarketImpact(transient_market_impact_gap={'ms': 100},
                                                                       trade_order_list=trade_order_list),
                                           MetricPermanentMarketImpact(permanent_market_impact_gap={'h': 1},
                                                                       trade_order_list=trade_order_list)],
                             results_form=[TimelineResultsForm(metric_name='slippage', by_date='date'),
                                           BarResultsForm(metric_name='slippage', aggregate_by_field='venue')],
                             benchmark_calcs=[BenchmarkArrival(), BenchmarkMarketSpreadToMid()],
                             trade_order_mapping=data_frame_trade_order_mapping, use_multithreading=False)

    # Dictionary of dataframes as output from TCA calculation
    dict_of_df = tca_engine.calculate_tca(tca_request)

    print(dict_of_df.keys())

    timeline_df = dict_of_df['timeline_' + trade_order_type + '_slippage_by_all']  # average slippage per day
    metric_df = dict_of_df[trade_order_type]['permanent_market_impact']  # permanent market impact for every trade

    print(metric_df.head(500))

    if PLOT:
        from chartpy import Chart, Style

        # plot slippage by timeline
        Chart(engine='plotly').plot(timeline_df)

        # plot market impact (per trade)
        Chart(engine='plotly').plot(metric_df.head(500))


def dataframe_compliance_tca_example():
    """Get a DataFrame of trades and apply compliance based TCA to it
    """

    tca_engine = TCAEngineImpl(version=tca_version)

    spread_to_mid_bp = 0.1
    trade_order_list = ['trade_df']

    # Read in CSV file as a DataFrame
    trade_df = DatabaseSourceCSV(trade_data_database_csv=csv_trade_order_mapping['trade_df']).fetch_trade_order_data()

    data_frame_trade_order_mapping = OrderedDict([('trade_df', trade_df)])

    ticker_list = FXConv().correct_unique_notation_list(trade_df['ticker'].unique().tolist())

    start_date = trade_df.index[0]; finish_date = trade_df.index[-1]

    # Specify the TCA request
    tca_request = TCARequest(start_date=start_date, finish_date=finish_date, ticker=ticker_list,
                             tca_type='aggregated', dummy_market=True,
                             trade_data_store='dataframe', market_data_store=market_data_store,

                             metric_calcs=[MetricSlippage(trade_order_list=trade_order_list),
                                           MetricTransientMarketImpact(transient_market_impact_gap={'ms': 100},
                                                                       trade_order_list=trade_order_list),
                                           MetricPermanentMarketImpact(permanent_market_impact_gap={'h': 1},
                                                                       trade_order_list=trade_order_list)],

                             benchmark_calcs=[  # add spread to mid fields for every market data spot
                                 BenchmarkMarketSpreadToMid(bid_mid_bp=spread_to_mid_bp, ask_mid_bp=spread_to_mid_bp),
                             ],

                             results_form=[
                                 # Display a table of all the anomalous trades by slippage (ie. outside bid/ask)
                                 TableResultsForm(market_trade_order_list=['trade_df'],
                                                  metric_name='slippage',
                                                  filter_by='worst_all',  # Order by the worst slippage
                                                  tag_value_combinations={'slippage_anomalous': 1.0},

                                                  # Only flag trades outside bid/ask
                                                  keep_fields=['executed_notional_in_reporting_currency', 'side'],

                                                  # Display only side and executed notionals
                                                  round_figures_by=None),

                                 # Get the total notional executed by broker (in reporting currency)
                                 BarResultsForm(market_trade_order_list=['trade_df'],  # trade
                                                aggregate_by_field='broker_id',  # aggregate by broker name
                                                # keep_fields=['executed_notional_in_reporting_currency', 'executed_notional', 'side'],
                                                metric_name='executed_notional_in_reporting_currency',
                                                # analyse notional
                                                aggregation_metric='sum',  # sum the notional
                                                scalar=1,  # no need for a multipler
                                                round_figures_by=0),  # round to nearest unit

                                 # Get average slippage per broker (weighted by notional)
                                 BarResultsForm(market_trade_order_list=['trade_df'],
                                                aggregate_by_field='broker_id',
                                                metric_name='slippage',
                                                aggregation_metric='mean',
                                                # keep_fields=['executed_notional_in_reporting_currency', 'executed_notional',
                                                #             'side'],
                                                weighting_field='executed_notional_in_reporting_currency',
                                                # weight results by notional
                                                scalar=10000.0,
                                                round_figures_by=2)
                             ],

                             # Aggregate the results (total notional and slippage) by broker
                             # into a single table for easy display to the user
                             join_tables=[JoinTables(
                                 tables_dict={'table_name': 'jointables_broker_id',

                                              # fetch the following calculated tables
                                              'table_list': [
                                                  'bar_trade_df_executed_notional_in_reporting_currency_by_broker_id',
                                                  'bar_trade_df_slippage_by_broker_id'],

                                              # append to the columns of each table
                                              'column_list': ['notional (rep cur)', 'slippage (bp)']
                                              })],

                             trade_order_mapping=data_frame_trade_order_mapping, use_multithreading=False)

    # Dictionary of dataframes as output from TCA calculation
    dict_of_df = tca_engine.calculate_tca(tca_request)

    # print all the output tables
    print(dict_of_df.keys())

    print('All trades')
    print(dict_of_df['trade_df'])

    print('Notional by broker ID')
    print(dict_of_df['bar_trade_df_executed_notional_in_reporting_currency_by_broker_id'])

    print('Notional by broker ID and weighted slippage')
    print(dict_of_df['jointables_broker_id'])

    print('Trades by worst slippage')
    print(dict_of_df['table_trade_df_slippage_by_worst_all'])

    from chartpy import Canvas, Chart

    broker_notional_chart = Chart(engine='plotly', df=dict_of_df['bar_trade_df_executed_notional_in_reporting_currency_by_broker_id'],
                         chart_type='bar', style=Style(title='Notional in USD per broker'))

    broker_slippage_chart = Chart(engine='plotly',
                         df=dict_of_df['bar_trade_df_slippage_by_broker_id'],
                         chart_type='bar', style=Style(title='Slippage by broker (bp)'))

    # Using plain template
    canvas = Canvas([[broker_notional_chart, broker_slippage_chart]])

    canvas.generate_canvas(silent_display=False, canvas_plotter='plain')

if __name__ == '__main__':
    import time
    start = time.time()

    dataframe_tca_example()
    dataframe_compliance_tca_example()

    finish = time.time()
    print('Status: calculated ' + str(round(finish - start, 3)) + "s")
