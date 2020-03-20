"""Queries what trades are available in the database
"""

from __future__ import print_function, division

__author__ = 'saeedamen'  # Saeed Amen / saeed@cuemacro.com

#
# Copyright 2018 Cuemacro
#
# See the License for the specific language governing permissions and limitations under the License.
#

from tcapy.conf.constants import Constants

constants = Constants()

if __name__ == '__main__':
    ### Fetch all the trades from SQL Server (irrespective of ticker) and every event-type
    from tcapy.data.datafactory import DataFactory
    from tcapy.analysis.tcarequest import TradeRequest
    from tcapy.analysis.algos.resultssummary import ResultsSummary

    data_factory = DataFactory()
    results_summary = ResultsSummary()

    start_date = '01 Mar 2018'; finish_date = '01 Apr 2018'

    trade_order_type_list = ['trade_df']
    query_fields = ['ticker', 'broker_id']

    for t in trade_order_type_list:
        trade_request = TradeRequest(start_date=start_date, finish_date=finish_date, data_store='ms_sql_server',
                                     trade_order_type=t)

        trade_order_df = data_factory.fetch_table(trade_request)
        query_dict = results_summary.query_trade_order_population(trade_order_df, query_fields=query_fields)

        print(query_dict)