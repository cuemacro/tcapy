from __future__ import division, print_function

__author__ = 'saeedamen' # Saeed Amen / saeed@cuemacro.com

#
# Copyright 2019 Cuemacro
#
# See the License for the specific language governing permissions and limitations under the License.
#

from flask import Flask, request, jsonify

# hack to get Flask RestPlus to work!
import werkzeug
werkzeug.cached_property = werkzeug.utils.cached_property

from flask_restplus import Resource, Api

import json
from plotly.utils import PlotlyJSONEncoder

from tcapy.analysis.tcaengine import TCAEngineImpl
from tcapy.analysis.tcarequest import TCARequest
from tcapy.util.fxconv import FXConv
from tcapy.util.loggermanager import LoggerManager
from tcapy.analysis.algos.metric import *

from tcapy.analysis.algos.metric import *
from tcapy.analysis.algos.resultsform import *

from tcapy.data.databasesource import DatabaseSourceCSVBinary

from collections import OrderedDict

tca_engine = TCAEngineImpl()

application = Flask(__name__)
api = Api(application,
          version='0.1',
          title='tcapy API',
          description='This is the API for tcapy',
)

@api.route('/tca_computation')
class TCAComputation(Resource):

    def get(self):
        logger = LoggerManager.getLogger(__name__)

        if request.content_type == 'application/json':
            json_input = request.json

            if 'trade_df' in json_input.keys() and 'username' in json_input.keys() and 'password' in json_input.keys():
                username = json_input['username']

                # TODO check passwords
                password = json_input['password']

                logger.info("Received API request from user: " + username)
                trade_df = json_input['trade_df']

                # assume that the user uploaded a binary CSV file/JSON
                trade_df = DatabaseSourceCSVBinary(trade_data_database_csv=trade_df).fetch_trade_order_data()

                data_frame_trade_order_mapping = OrderedDict([('trade_df', trade_df)])

                start_date = trade_df.index[0];
                finish_date = trade_df.index[-1]

                ticker_val = FXConv().correct_unique_notation_list(trade_df['ticker'].unique().tolist())

                if 'ticker' in json_input.keys():
                    ticker_val = list(set(ticker_val).intersection(set(json_input['ticker'])))

                for t in ticker_val:
                    if t not in constants.available_tickers_dictionary['All']:
                        ticker_val.remove(t)

                metric_val = 'slippage'

                results_form = [
                    # show the distribution of the selected metric for trades weighted by notional
                    # aggregated by ticker and then by venue
                    DistResultsForm(market_trade_order_list=['trade_df'], metric_name=metric_val,
                                    aggregate_by_field=['ticker', 'broker_id', 'venue'],
                                    weighting_field='executed_notional_in_reporting_currency'),

                    # display the timeline of metrics average by day (and weighted by notional)
                    TimelineResultsForm(market_trade_order_list=['trade_df'], by_date='date',
                                        metric_name=metric_val,
                                        aggregation_metric='mean',
                                        aggregate_by_field=['ticker'], scalar=10000.0,
                                        weighting_field='executed_notional_in_reporting_currency'),

                    # display a bar chart showing the average metric weighted by notional and aggregated by ticker
                    # venue
                    BarResultsForm(market_trade_order_list=['trade_df'],
                                   metric_name=metric_val,
                                   aggregation_metric='mean',
                                   aggregate_by_field=['ticker', 'venue', 'broker_id'], scalar=10000.0,
                                   weighting_field='executed_notional_in_reporting_currency'),

                    # create a table the markout of every trade
                    TableResultsForm(market_trade_order_list=['trade_df'], metric_name='markout', filter_by='all',
                                     replace_text={'markout_': '', 'executed_notional': 'exec not',
                                                   'notional_currency': 'exec not cur'},
                                     keep_fields=['executed_notional', 'side', 'notional_currency'],
                                     scalar={'all': 10000.0, 'exclude': ['executed_notional', 'side']},
                                     round_figures_by={'all': 2, 'executed_notional': 0, 'side': 0},
                                     weighting_field='executed_notional')
                ]

                tca_request=TCARequest(start_date=start_date, finish_date=finish_date, ticker=ticker_val,
                                           tca_type='aggregated', summary_display='candlestick',
                                           market_data_store='arctic-ncfx', trade_data_store='dataframe',
                                           trade_order_mapping=data_frame_trade_order_mapping,
                                           metric_calcs=[MetricSlippage(), MetricMarkout(trade_order_list=['trade_df'])],
                                           results_form=results_form, dummy_market=True, use_multithreading=True)


                try:
                    dict_of_df = tca_engine.calculate_tca(tca_request)
                    dict_of_df = UtilFunc().convert_dict_of_dataframe_to_json(dict_of_df)

                except Exception as e:
                    logger.error("Failed to complete request for user: " + username + " - " + str(e))

                    return "Failed to complete request"

                logger.info("Completed API request from user: " + username)

            else:
                return 'Missing fields in request'

            return jsonify({'response': 200, 'results': dict_of_df})
        else:
            return "Unsupported media type, only accepts JSON"

        ##except Exception as e:
        ##    return {'Encountered exception during calculation: ' + str(e)}

if __name__ == '__main__':
    application.run(debug=True, port=9000)