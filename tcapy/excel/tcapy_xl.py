__author__ = 'saeedamen' # Saeed Amen / saeed@cuemacro.com

#
# Copyright 2020 Cuemacro Ltd. - http//www.cuemacro.com / @cuemacro
#
# See the License for the specific language governing permissions and limitations under the License.
#

import time
import datetime

import xlwings as xw

from tcapy.analysis.tcarequest import TCARequest
from tcapy.analysis.tcaengine import TCAEngineImpl

from tcapy.analysis.algos.metric import *
from tcapy.analysis.algos.resultsform import *
from tcapy.analysis.algos.benchmark import *

from tcapy.vis.tcaresults import TCAResults
from tcapy.vis.report.computationreport import JinjaRenderer, XlWingsRenderer
from tcapy.vis.report.tcareport import TCAReport

tca_engine = TCAEngineImpl()

# For testing your xlwings installation, leave the "demo" functions
def hello_xlwings():
    wb = xw.Book.caller()
    wb.sheets[0].range("C2").value = "Hello xlwings!"

@xw.func
def hello(name):
    return "hello {0}".format(name)

def run_tcapy_computation():
    start = time.time()

    # Collect inputs from Excel & create a reference to the calling Excel Workbook
    trade_df_sht = xw.Book.caller().sheets[1]
    trade_df_out_sht = xw.Book.caller().sheets[2]
    results_sht = xw.Book.caller().sheets[3]

    # Get the ticker and the API key from Excel
    start_date = trade_df_sht.range('start_date').value
    finish_date = trade_df_sht.range('finish_date').value
    ticker = trade_df_sht.range('ticker').value
    use_multithreading = trade_df_sht.range('use_multithreading').value
    pdf_path = trade_df_sht.range('pdf_path').value

    if "," in ticker:
        ticker = ticker.split(",")

    market_data_store = trade_df_sht.range('market_data_store').value

    # Get the trade_df table as a DataFrame (careful to parse the dates)
    trade_df = trade_df_sht.range('trade_df_input').options(pd.DataFrame, index=False).value
    trade_df = trade_df.dropna(subset=['date'])
    trade_df = trade_df.loc[:, trade_df.columns.notnull()]

    # Let Pandas parse the date (not Excel, which might miss off some of the field
    # which is important calculation of metrics etc.)
    trade_df['date'] = pd.to_datetime(trade_df['date'])
    trade_df = trade_df.set_index('date')

    # Push trades as DataFrame to tcapy
    data_frame_trade_order_mapping = {'trade_df' : trade_df}

    trade_order_list = ['trade_df']

    # Create TCARequest object
    tca_request = TCARequest(start_date=start_date, finish_date=finish_date, ticker=ticker,
                             trade_data_store='dataframe',
                             trade_order_mapping=data_frame_trade_order_mapping,
                             market_data_store=market_data_store,
                             tca_type='aggregated',

                             metric_calcs=[  # Calculate the slippage for trades/order
                                 MetricSlippage(trade_order_list=trade_order_list),

                                 # Calculate the shorter and longer term market impact after every trade/order
                                 MetricTransientMarketImpact(transient_market_impact_gap={'ms': 100},
                                                             trade_order_list=trade_order_list),
                                 MetricPermanentMarketImpact(permanent_market_impact_gap={'h': 1},
                                                             trade_order_list=trade_order_list)],

                             results_form=[  # Aggregate the slippage average by date and hour
                                 TimelineResultsForm(metric_name='slippage', by_date='datehour', scalar=10000.0),

                                 # Aggregate the total executed notional in reporting currency (usually USD)
                                 # for every hour
                                 TimelineResultsForm(metric_name='executed_notional_in_reporting_currency',
                                                     by_date='datehour',
                                                     aggregation_metric='sum', scalar=1.0),

                                 # Aggregate the average slippage on trades by venue
                                 HeatmapResultsForm(metric_name=['slippage', 'transient_market_impact'],
                                                    aggregate_by_field=['venue', 'ticker'], scalar=10000.0,
                                                    trade_order_list='trade_df'),

                                 # Aggregate the average slippage on trades by venue
                                 BarResultsForm(metric_name='slippage', aggregate_by_field='venue', scalar=10000.0,
                                                trade_order_list='trade_df'),

                                 # Aggregate the average slippage on trades/orders by broker_id
                                 BarResultsForm(metric_name='slippage', aggregate_by_field='broker_id', scalar=10000.0),

                                 # Aggregate the average slippage on trades/orders by broker_id
                                 DistResultsForm(metric_name='slippage', aggregate_by_field='side', scalar=10000.0),

                                 # Create a scatter chart of slippage vs. executed notional
                                 ScatterResultsForm(
                                     scatter_fields=['slippage', 'executed_notional_in_reporting_currency'],
                                     scalar={'slippage': 10000.0})],

                             benchmark_calcs=[  # At the arrival price for every trade/order
                                 BenchmarkArrival(),

                                 # At the spread at the time of every trade/order
                                 BenchmarkMarketSpreadToMid()],

                             summary_display='candlestick',
                             use_multithreading=use_multithreading,
                             dummy_market=True)

    # Kick off calculation and get results
    dict_of_df = tca_engine.calculate_tca(tca_request)

    # Create PDF report
    tca_results = TCAResults(dict_of_df, tca_request)
    tca_results.render_computation_charts()

    tca_report = TCAReport(tca_results, renderer=JinjaRenderer())
    tca_report.create_report(output_filename=pdf_path, output_format='pdf', offline_js=False)

    tca_report = TCAReport(tca_results, renderer=XlWingsRenderer(xlwings_sht=results_sht))
    tca_report.create_report(output_format='xlwings')

    finish = time.time()

    trade_df_sht.range('calculation_status').value = \
        'Calculated ' + str(round(finish - start, 3)) + "s at " + str(datetime.datetime.utcnow())

    # Output results
    trade_df_out_sht.range('trade_df_output').clear_contents()

    # Print trade_df + additional fields to the spreadsheet (eg. slippage)
    trade_df_out_sht.range("trade_df_output").value = dict_of_df['trade_df']

if __name__ == '__main__':
    xw.serve()
