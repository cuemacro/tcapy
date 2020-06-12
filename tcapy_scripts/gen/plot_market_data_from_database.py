"""Script to fetch market data from database and plot in Plotly. It might often be the case you need to get market data
for other purposes (eg. to plot in Excel etc.). Note, given it's high frequency data we need to careful when fetching large
amounts to plot.
"""

from __future__ import division, print_function

__author__ = 'saeedamen'  # Saeed Amen / saeed@cuemacro.com

#
# Copyright 2020 Cuemacro Ltd. - http//www.cuemacro.com / @cuemacro
#
# See the License for the specific language governing permissions and limitations under the License.
#

from chartpy import Chart, Style

from tcapy.analysis.tcamarkettradeloaderimpl import TCAMarketTradeLoaderImpl
from tcapy.analysis.tcarequest import MarketRequest
from tcapy.vis.displaylisteners import PlotRender
from tcapy.conf.constants import Constants

constants = Constants()

if __name__ == '__main__':
    # 'arctic' or 'pystore'
    database_dialect = 'arctic'

    # 'dukascopy' or 'ncfx'
    data_vendor = 'dukascopy'

    ticker = 'EURUSD'

    # Warning for high frequency data file sizes might be very big, so you may need to reduce this!
    market_request = MarketRequest(start_date='01 Jan 2020', finish_date='01 Feb 2020', ticker=ticker,
                                   data_store=database_dialect + '-' + data_vendor)

    tca_market_trade_loader = TCAMarketTradeLoaderImpl()

    df = tca_market_trade_loader.get_market_data(market_request)

    # Grab a Plotly figure of the data
    fig = PlotRender().plot_timeline(df, title=ticker)

    # Generate HTML file of Plotly figure
    Chart(engine='plotly').plot(fig, style=Style(html_file_output='test.html'))

    print(df)
