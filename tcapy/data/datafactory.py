__author__ = 'saeedamen'  # Saeed Amen / saeed@cuemacro.com

#
# Copyright 2017 Cuemacro Ltd. - http//www.cuemacro.com / @cuemacro
#
# See the License for the specific language governing permissions and limitations under the License.
#


from tcapy.data.databasesource import *

from tcapy.util.loggermanager import LoggerManager
from tcapy.conf.constants import Constants
from tcapy.util.timeseries import TimeSeriesOps

constants = Constants()

from tcapy.analysis.tcarequest import TradeRequest, MarketRequest
from tcapy.util.mediator import Mediator
from datetime import timedelta

import numpy as np

class DataFactory(object):
    """This class takes in DataRequests and then gets the underlying DatabaseSource (eg. CSV) to
    fetch a DataFrame filled with market data or trade/order data. Also calls the underlying DataNorm object to shift
    the DataFrame (or a user specified one) which we have fetched into an appropriate format for tcapy.

    """

    def __init__(self, version=constants.tcapy_version):
        self._version = version

    def fetch_table(self, data_request):
        """Fetches table from underlying DatabaseSource

        Parameters
        ----------
        data_request : DataRequest
            Request for data with start/finish date etc.

        Returns
        -------
        DataFrame
        """
        # Fetch table from the underlying database (CSV, SQL or RESTful etc.)
        logger = LoggerManager().getLogger(__name__)

        data_norm = data_request.data_norm

        if data_norm is None:
            data_norm = Mediator.get_data_norm(version=self._version)

        # Where do we get data from?
        database_source = Mediator.get_database_source_picker().get_database_source(data_request)

        if database_source is None:
            Exception("User asked for an unsupported database source")

        # Extract the start/finish dates and ticker we wish to download data for
        start_date = data_request.start_date
        finish_date = data_request.finish_date
        ticker = data_request.ticker

        # Are we requesting market data or trade/order data of our own executions?
        if isinstance(data_request, MarketRequest):
            df = database_source.fetch_market_data(start_date=start_date, finish_date=finish_date, ticker=ticker,
                                                   table_name=data_request.market_data_database_table)

            df = data_norm.normalize_market_data(df, None, data_request)
        elif isinstance(data_request, TradeRequest):

            trade_order_type = data_request.trade_order_type
            trade_order_mapping = data_request.trade_order_mapping
            trade_data_database_name = data_request.trade_data_database_name

            if data_request.data_store == 'csv' and trade_order_type != None and trade_order_mapping != None:
                df = database_source.fetch_trade_order_data(start_date=start_date, finish_date=finish_date,
                                                            ticker=ticker,
                                                            table_name=trade_order_mapping[trade_order_type])
            elif trade_order_mapping is not None:
                df = database_source.fetch_trade_order_data(start_date=start_date, finish_date=finish_date,
                                                            ticker=ticker,
                                                            table_name=trade_order_mapping[trade_order_type],
                                                            database_name=trade_data_database_name)
            else:
                # Otherwise we have a CSV file without any sort of mapping, which we assume only contains trade_df data
                df = database_source.fetch_trade_order_data(start_date=start_date, finish_date=finish_date,
                                                            ticker=ticker)

            df = data_norm.normalize_trade_data(df, None, data_request)

        if df is None:
            pass

        if df is not None and df.empty:
            logger.warning('Dataframe empty for ticker ' + ticker)

        return df

class DataNorm(object):
    """This class can be used to normalise the data, eg. change _tag names, adjust timing etc.

    """

    def normalize_market_data(self, df, dataset, data_request):
        df = Mediator.get_time_series_ops().localize_as_UTC(df)

        # For each dataset have a different field mapping (get field mapping for that dataset from stored CSV files)

        # Convert vendor specific field names to the Cuemacro names

        # Convert vendor specific asset names (eg. GBP=) to Cuemacro standard names (GBPUSD)

        # The dataset is very dense, we assume it is stored on disk ordered (Arctic only allows this)
        # df = df.sort_index()

        return self.offset_data_ms(df, data_request)

    def normalize_trade_data(self, df, dataset, data_request):

        if df is None: return None

        # For cancelled trades the trade price might be recorded as "zero" or a negative price, which is invalid, make these NaNs
        if 'executed_price' in df.columns:
            # df['executed_price'][df['executed_price'] <= 0] = np.nan
            df.loc[df['executed_price'] <= 0, 'executed_price'] = np.nan

        # Rename fields if necessary
        if 'executed_notional_currency' in df.columns:
            df = df.rename(columns={'executed_notional_currency' : 'notional_currency'})

        # Convert buy/sell to -1/+1

        # TODO do regex/case insensitive version
        # vals_to_replace = {'buy': 1, 'sell' : -1, 'Buy' : 1, 'Sell' : -1, 'BUY' : 1, 'SELL' : -1}
        # df['side'] = df['side'].map(vals_to_replace)

        df['side'].replace('buy', 1, inplace=True)
        df['side'].replace('sell', -1, inplace=True)
        df['side'].replace('Buy', 1, inplace=True)
        df['side'].replace('Sell', -1, inplace=True)
        df['side'].replace('BUY', 1, inplace=True)
        df['side'].replace('SELL', -1, inplace=True)

        if 'event_type' in df.columns:
            df['event_type'].replace('execution', 'trade', inplace=True)

        # Also assume selected date columns are UTC (eg. benchmark start and finish dates for the orders)
        df = Mediator.get_time_series_ops().localize_cols_as_UTC(df, constants.date_columns, index=True).sort_index()

        df = self.offset_data_ms(df, data_request)

        return df

    def offset_data_ms(self, df, data_request):
        """Offsets all date/time columns by a certain number of milliseconds

        Parameters
        ----------
        df : DataFrame
            Market/trade data to be

        data_request : DataRequest
            Associated data request (contains field for milliseconds pertubation)

        Returns
        -------
        DataFrame
        """

        data_offset_ms = data_request.data_offset_ms

        if data_offset_ms is None: return df
        if data_offset_ms == 0: return df

        df.index = df.index + timedelta(milliseconds=data_offset_ms)

        for d in constants.date_columns:
            if d in df.columns:
                df[d] = df[d] + timedelta(milliseconds=data_offset_ms)

        return df

