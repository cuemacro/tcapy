from __future__ import division
from __future__ import print_function

__author__ = 'saeedamen'  # Saeed Amen / saeed@cuemacro.com

#
# Copyright 2017 Cuemacro Ltd. - http//www.cuemacro.com / @cuemacro
#
# See the License for the specific language governing permissions and limitations under the License.
#
from random import randint

import datetime

import pandas as pd
import numpy as np

from tcapy.conf.constants import *
from tcapy.util.timeseries import TimeSeriesOps, RandomiseTimeSeries
from tcapy.util.utilfunc import UtilFunc

from tcapy.data.databasesource import DatabaseSourceArctic, DatabaseSourceMSSQLServer

from tcapy.analysis.tcarequest import MarketRequest
from tcapy.util.loggermanager import LoggerManager
from tcapy.util.mediator import Mediator

constants = Constants()

class DataTestCreator(object):
    """This class copies market data/trade data to our database (by default: Arctic/MongoDB for market data and
    MSSQL for trade data). It generates randomised test trades/orders based upon the market data, randomly perturbing
    the bid/ask to simulate a traded price.

    """

    def __init__(self, market_data_postfix='dukascopy', csv_market_data=None, write_to_db=True):
        if csv_market_data is None:
            self._market_data_source = 'arctic-' + market_data_postfix
        else:
            self._market_data_source = csv_market_data

        self._tca_market = Mediator.get_tca_market_trade_loader()

        # Assumes MongoDB for tick data and MSSQL for trade/order data
        if write_to_db:
            self._database_source_market = DatabaseSourceArctic(postfix=market_data_postfix) # market data source
            self._database_source_trade = DatabaseSourceMSSQLServer()                        # trade data source

            self._market_data_database_name = constants.arctic_market_data_database_name
            self._market_data_database_table = constants.arctic_market_data_database_table

            self._trade_data_database_name = constants.ms_sql_server_trade_data_database_name

        self.time_series_ops = TimeSeriesOps()
        self.rand_time_series = RandomiseTimeSeries()

    def populate_test_database_with_csv(self, csv_market_data=None, ticker=None, csv_trade_data=None,
                                        if_exists_market_table='append', if_exists_market_ticker='replace',
                                        if_exists_trade_table='replace',
                                        market_data_postfix='dukascopy', remove_market_duplicates=False):
        """Populates both the market database and trade database with market data and trade/order data respectively, which
        have been sourced in CSV/HDF5 files.

        Parameters
        ----------
        csv_market_data : str (list)
            Path of CSV/HDF5 file with market data

        ticker : str (list)
            Ticker for market data

        csv_trade_data : dict
            Dictionary with name of trade/order and associated path of CSV/HDF5 file with trade/order data

        if_exists_market_table : str
            'replace' - deletes whole market data table
            'append' (default) - adds to existing market data

        if_exists_market_ticker : str
            'replace' (default) - deletes existing data for the ticker
            'append' - appends data for this this

        if_exists_trade_table : str
            'replace' - deletes data in trade table, before writing

        market_data_postfix : str (default 'dukascopy')
            data source for market data (typically broker or venue name)

        remove_market_duplicates : bool (default: False)
            Should we remove any duplicated values in market data (for TCA purposes, we can usually remove duplicated values
            However, we need to be careful when using richer market data (eg. with volume data), where consecutive prices
            might be the same but have different volume/other fields

        Returns
        -------

        """

        logger = LoggerManager.getLogger(__name__)
        
        # Populate the market data (eg. spot data)
        if csv_market_data is not None:
            self._database_source_market.set_postfix(market_data_postfix)
            logger.info('Writing market data to database')
            self._database_source_market.convert_csv_to_table(csv_market_data, ticker, self._market_data_database_table,
                                                              database_name=self._market_data_database_name,
                                                              if_exists_table=if_exists_market_table,
                                                              if_exists_ticker=if_exists_market_ticker,
                                                              remove_duplicates=remove_market_duplicates)

        # Populate the test trade/order data (which will have been randomly generated)
        if csv_trade_data is not None:
            logger.info('Writing trade data to database')

            # Allow for writing of trades + orders each to a different database table
            if isinstance(csv_trade_data, dict):
                for key in csv_trade_data.keys():
                    # csv file name, trade/order name (eg. trade_df)
                    self._database_source_trade.convert_csv_to_table(
                        csv_trade_data[key], None, key, database_name=self._trade_data_database_name,
                        if_exists_table=if_exists_trade_table)

            # Otherwise simply assume we are writing trade data
            else:
                logger.error("Specify trade/orders hierarchy")

        logger.info('Completed writing data to database')

    def create_test_trade_order(self, ticker, start_date='01 Jan 2016', finish_date='01 May 2018',
                                order_min_size=0.5 * constants.MILLION,
                                order_max_size=20.0 * constants.MILLION,
                                number_of_orders_min_per_year=252 * 20,
                                number_of_orders_max_per_year=252 * 200):

        """Create a randomised list of orders & trade using indicative market data as a source (and perturbing the
        execution prices, within various constraints, such as the approximate size of orders trades, the orders per year

        Parameters
        ----------
        ticker : str
            Ticker

        start_date : str
            Start date of the orders

        finish_date : str
            Finish date of the orders

        order_min_size : float
            Minimum size of orders

        order_max_size : float
            Maximum size of orders

        number_of_orders_min_per_year : int
            Minimum orders per year

        number_of_orders_max_per_year : int
            Maximum orders per year

        Returns
        -------
        DataFrame
        """
        logger = LoggerManager.getLogger(__name__)

        if isinstance(ticker, str):
            ticker = [ticker]

        order_list = []
        trade_list = []

        start_date = self.time_series_ops.date_parse(start_date, assume_utc=True)
        finish_date = self.time_series_ops.date_parse(finish_date, assume_utc=True)
        util_func = UtilFunc()

        # Make this parallel? but may have memory issues
        for tick in ticker:

            logger.info("Loading market data for " + tick)

            # split into yearly chunks (otherwise can run out of memory easily)
            date_list = util_func.split_date_single_list(start_date, finish_date, split_size='yearly',
                                                         add_partial_period_start_finish_dates=True)

            # TODO do in a batch fashion
            for i in range(0, len(date_list) - 1):
                df = self._tca_market.get_market_data(
                    MarketRequest(start_date=date_list[i], finish_date=date_list[i + 1], ticker=tick,
                                  data_store=self._market_data_source))


                # self.database_source_market.fetch_market_data(start_date = start_date, finish_date = finish_date, ticker = tick)

                # Need to make sure there's sufficient market data!
                if df is not None:
                    if len(df.index) >= 2:
                        # Get the percentage of the year represented by the difference between the start and finish dates
                        year_perc = float((df.index[-1] - df.index[0]).seconds / (24.0 * 60.0 * 60.0)) / 365.0

                        logger.info("Constructing randomised trades for " + tick)

                        number_of_orders_min = int(year_perc * number_of_orders_min_per_year)
                        number_of_orders_max = int(year_perc * number_of_orders_max_per_year)

                        # Split up the data frame into equally sized chunks
                        df_orders = self._derive_order_no(self._strip_columns(df, tick), number_of_orders_min, number_of_orders_max)

                        # Don't want a memory leak, so delete this as soon possible from memory!
                        del df

                        # order_counter = 0

                        logger.info("Now beginning order construction for " + tick)

                        # For each order create randomised associated trades
                        # group together all the trades per day as orders
                        for df_order in df_orders:

                            # Set duration of the grandparent order (find randomised start/finish time)
                            # somewhere between 0-25% for start, and 75% to 100% for end point
                            df_order = self.rand_time_series.randomly_truncate_data_frame_within_bounds(df_order, start_perc=0.25,
                                                                                                        finish_perc=0.75)

                            logger.debug("Creating order between " + str(df_order.index[0]) + " - " + str(df_order.index[-1]))

                            # Assume all orders/trades are in the same direction (which is randomly chosen)
                            buy_sell = randint(0, 1)

                            # Sell trades
                            if buy_sell == 0:
                                side_no = -1
                                side = 'bid'

                            # Buy trades
                            else:
                                side_no = 1
                                side = 'ask'

                            magnitude = 10000.0 * 2

                            if tick == 'USDJPY': magnitude = 100.0 * 2.0

                            if randint(0, 100) > 97:
                                new_tick = tick[3:6] + tick[0:3]

                                if 'ticker' in df_order.columns:
                                    df_order['ticker'] = new_tick

                                if 'bid' in df_order.columns and 'ask' in df_order.columns:
                                    ask = 1.0 / df_order['bid']
                                    bid = 1.0 / df_order['ask']

                                    df_order['bid'] = bid
                                    df_order['ask'] = ask

                                df_order['mid'] = 1.0 / df_order['mid']
                            else:
                                new_tick = tick

                            # Get 'bid' for sells, and 'ask' for buys
                            df_order['trade_value'] = df_order[side]

                            # We want to simulate the executions by perturbing the buys randomly
                            df_order = self.rand_time_series.randomly_perturb_column(df_order, column='trade_value',
                                                                                     magnitude=magnitude)

                            # Assume notional is in base currency in vast majority of cases
                            if randint(0, 100) > 97:
                                notional_currency = new_tick[3:6]
                            else:
                                notional_currency = new_tick[0:3]

                            notional_multiplier = 1.0

                            if notional_currency == 'JPY':
                                notional_multiplier = 100.0

                            # Randomly choose a realistic order notional
                            # This will later be subdivided into trade notional
                            order_notional = randint(order_min_size*notional_multiplier, order_max_size*notional_multiplier)

                            order_additional_attributes = {'broker_id': constants.test_brokers_dictionary['All'],
                                                           'broker_sub_id': constants.test_sub_brokers_dictionary['All'],
                                                           'algo_id': constants.test_algos_dictionary['All'],
                                                           'algo_settings': 'default',
                                                           }

                            # Construct an order and add it to list
                            ind_order = self._construct_order(df_order, order_type='order',
                                                              notional=order_notional, notional_currency=notional_currency,
                                                              side=side_no, tick=new_tick,
                                                              additional_attributes=order_additional_attributes)

                            order_list.append(ind_order)

                            trade_additional_attributes = self.grab_attributes_from_trade_order(
                                ind_order, ['broker_id', 'broker_sub_id', 'algo_id', 'algo_settings'])

                            # Now create all the broker messages for the order

                            # These will consist firstly of placement messages
                            # then potentionally cancels, cancel/replace and in most cases we randomly assign trade fills
                            trade_list = self._create_trades_from_order(
                                trade_list=trade_list, df_order=df_order, tick=new_tick, ind_order=ind_order, side_no=side_no,
                                order_notional=order_notional, notional_currency=notional_currency,
                                additional_attributes=trade_additional_attributes)

                            # order_counter = order_counter + 1

        # Aggregate all the lists into DataFrames (setting 'date' as the index)

        # For the trade dataframe also drop the 'index' column which was previous used to ensure that fills, were after placements
        trade_order_dict = {'trade_df': self.time_series_ops.aggregate_dict_to_dataframe(trade_list, 'date', 'index'),
                            'order_df': self.time_series_ops.aggregate_dict_to_dataframe(order_list, 'date')}

        return trade_order_dict

    def _create_trades_from_order(self, trade_list=None, df_order=None, tick=None, ind_order=None, side_no=None,
                                  order_notional=None,
                                  notional_currency=None, additional_attributes=None):

        trade_notional = order_notional

        # Assume placement at start of order (a placement will have the order notional)
        placement_event = self.construct_trade(df_order,
                                               order_notional=order_notional,
                                               execution_venue=constants.test_venues_dictionary['All'], order=ind_order,
                                               side=side_no, tick=tick, event_type='placement',
                                               notional_currency=notional_currency,
                                               additional_attributes=additional_attributes)

        trade_list.append(placement_event)

        # Randomly choose an event (cancel/replace + fill, cancel or fill)
        i = randint(0, 1000)

        # Very rare event, same timestamp for a trade, same size too (but different ID)
        if i < 1:

            # executed trade
            fill_event = self.construct_trade(df_order, order=ind_order,
                                              order_notional=order_notional,
                                              execution_venue=placement_event['venue'],
                                              notional_currency=notional_currency,
                                              executed_notional=int(float(trade_notional) * 0.5),
                                              side=side_no, tick=tick, event_type='trade',
                                              index=min(len(df_order.index), 5),
                                              additional_attributes=additional_attributes)

            trade_list.append(fill_event)

            fill_event = self.construct_trade(df_order.copy(), order=ind_order,
                                              order_notional=order_notional,
                                              execution_venue=placement_event['venue'],
                                              notional_currency=notional_currency,
                                              executed_notional=int(float(trade_notional) * 0.5),
                                              side=side_no, tick=tick, event_type='trade',
                                              index=min(len(df_order.index), 5),
                                              additional_attributes=additional_attributes)

            trade_list.append(fill_event)
        elif i < 50:
            # Cancel/replace event
            cancel_replace_index = randint(1, min(len(df_order.index), 20))

            cancel_replace_event = self.construct_trade(
                df_order, order=ind_order, execution_venue=placement_event['venue'],
                notional_currency=notional_currency,
                side=side_no, tick=tick, event_type='cancel/replace', index=cancel_replace_index,
                additional_attributes=additional_attributes)

            trade_list.append(cancel_replace_event)

            fill_event_index = randint(cancel_replace_index + 1, min(len(df_order.index), 50))

            # Executed fill event
            fill_event = self.construct_trade(df_order, order=ind_order,
                                              order_notional=order_notional,
                                              execution_venue=placement_event['venue'],
                                              executed_notional=trade_notional,
                                              notional_currency=notional_currency,
                                              side=side_no, tick=tick, event_type='trade', index=fill_event_index,
                                              additional_attributes=additional_attributes)

            trade_list.append(fill_event)

        # Rare event, full cancellation of order
        elif i < 60:
            cancel_index = randint(1, min(len(df_order.index), 20))

            cancel_event = self.construct_trade(df_order, order=ind_order,
                                                execution_venue=placement_event['venue'],
                                                executed_notional=0,
                                                notional_currency=notional_currency,
                                                side=side_no, tick=tick, event_type='cancel',
                                                index=cancel_index, additional_attributes=additional_attributes)

            trade_list.append(cancel_event)

        elif i < 80:
            # Where we have two trade fills for a single child order of different sizes
            perc = float(randint(5, 95)) / 100.0

            # executed trade
            fill_event = self.construct_trade(df_order, order=ind_order,
                                              execution_venue=placement_event['venue'],
                                              notional_currency=notional_currency,
                                              executed_notional=int(float(trade_notional) * perc),
                                              side=side_no, tick=tick, event_type='trade',
                                              index=randint(1,
                                                            min(len(df_order.index), 50)),
                                              additional_attributes=additional_attributes)

            trade_list.append(fill_event)

            fill_event = self.construct_trade(df_order, order=ind_order,
                                              execution_venue=placement_event['venue'],
                                              notional_currency=notional_currency,
                                              executed_notional=int(float(trade_notional) * (1.0 - perc)),
                                              side=side_no, tick=tick, event_type='trade',
                                              index=randint(fill_event['index'],
                                                            min(len(df_order.index), 100)),
                                              additional_attributes=additional_attributes)

            trade_list.append(fill_event)

        # Most common event, single trade/fill
        else:
            # Executed trade
            fill_event = self.construct_trade(df_order, order=ind_order, order_notional=order_notional,
                                              execution_venue=placement_event['venue'],
                                              notional_currency=notional_currency,
                                              executed_notional=trade_notional,
                                              side=side_no, tick=tick, event_type='trade',
                                              index=randint(1,
                                                            min(len(df_order.index), 50)),
                                              additional_attributes=additional_attributes)

            trade_list.append(fill_event)

        return trade_list

    def _derive_order_no(self, df, orders_min, orders_max):
        df_chunks_list = self.time_series_ops.split_array_chunks(df, chunks=randint(orders_min, orders_max))

        if isinstance(df_chunks_list, pd.DataFrame):
            return [df_chunks_list]

        return df_chunks_list

    def _create_unique_trade_id(self, order_type, ticker, datetime_input):
        return order_type + "_" + ticker + str(datetime_input) + "_" + str(datetime.datetime.utcnow()) + '_' + str(
            randint(0, 100000))

    def _construct_order(self, df, order_type=None, notional=None, notional_currency=None, side=None, tick=None,
                         additional_attributes=None, **kwargs):

        order = {}

        # For internal purposes
        order['ticker'] = tick
        order['notional'] = notional

        order['notional_currency'] = notional_currency

        order['side'] = side

        order['date'] = df.index[0]
        order['benchmark_date_start'] = df.index[0]
        order['benchmark_date_end'] = df.index[-1]

        order['price_limit'] = df['mid'][0]
        order['arrival_price'] = df['mid'][0]

        order['portfolio_id'] = self.add_random_sample(constants.test_portfolios_dictionary['All'])
        order['portfolio_manager_id'] = self.add_random_sample(constants.test_portfolio_managers_dictionary['All'])
        order['trader_id'] = self.add_random_sample(constants.test_traders_dictionary['All'])
        order['account_id'] = self.add_random_sample(constants.test_accounts_dictionary['All'])

        order['id'] = self._create_unique_trade_id(order_type, tick, order['date'])

        kwargs['order'] = order

        order = self.additional_order_processing(**kwargs)

        # Add additional randomized attributes
        if additional_attributes is not None:

            # Merge list of additional attributes
            if isinstance(additional_attributes, list):
                result_dict = {}
                for d in additional_attributes:
                    result_dict.update(d)

                additional_attributes = result_dict

            for k in additional_attributes.keys():

                additional = additional_attributes[k]

                if isinstance(additional, list):
                    additional = self.add_random_sample(additional)

                order[k] = additional

        return order

    def additional_order_processing(self, **kwargs):

        return kwargs['order']

    def construct_trade(self, df, order_notional=None, executed_notional=None, notional_currency=None,
                        execution_venue=None, side=None, order=None,
                        tick=None, event_type=None,
                        additional_attributes=None, index=0):

        trade = {}

        if order_notional is None:
            order_notional = 0

        trade['order_notional'] = order_notional
        trade['notional_currency'] = notional_currency
        trade['ticker'] = tick
        trade['side'] = side
        trade['index'] = index
        trade['date'] = df.index[index]
        trade['market_bid'] = df['bid'][index]
        trade['market_ask'] = df['ask'][index]
        trade['market_mid'] = df['mid'][index]
        trade['price_limit'] = df['mid'][index]

        trade['event_type'] = event_type

        trade['executed_price'] = 0
        trade['venue'] = execution_venue
        trade['executed_notional'] = 0

        if event_type == 'trade':
            trade['executed_notional'] = executed_notional

            try:
                if np.isnan(trade['executed_notional']):
                    pass

            except:
                print('w')

            trade['executed_price'] = df['trade_value'][index]

        trade['venue'] = self.add_random_sample(constants.test_venues_dictionary['All'])

        if order is not None:
            trade[constants.order_name + '_pointer_id'] = order['id']
            trade['price_limit'] = order['price_limit']

            trade['portfolio_id'] = order['portfolio_id']
            trade['portfolio_manager_id'] = order['portfolio_manager_id']
            trade['trader_id'] = order['trader_id']
            trade['account_id'] = order['account_id']

        trade['id'] = self._create_unique_trade_id('execution', tick, trade['date'])

        if additional_attributes is not None:
            for k in additional_attributes.keys():
                trade[k] = additional_attributes[k]

        return trade

    def add_random_sample(self, lst):

        return lst[randint(0, len(lst) - 1)]

    def grab_attributes_from_trade_order(self, trade_order, attributes):

        dict = {}

        for a in attributes:
            dict[a] = trade_order[a]

        return dict

    def _strip_columns(self, df, tick):

        # filter market data so only includes specific asset (Arctic won't have this) and during "main" FX hours
        # exclude any Saturday data
        if 'ticker' in df.columns:
            df = df[(df.index.hour >= 6) & (df.index.hour < 21) & (df.index.dayofweek != 5) & (df['ticker'] == tick)]
        else:
            df = df[(df.index.hour >= 6) & (df.index.hour < 21) & (df.index.dayofweek != 5)]

        keep_cols = ['bid', 'ask', 'mid']

        remove_cols = []

        for k in df.columns:
            if k not in keep_cols:
                remove_cols.append(k)

        if remove_cols != []:
            df.drop(remove_cols, inplace=True, axis=1)

        # Ensure that the market is data is properly sorted
        df.sort_index(inplace=True)

        # Calculate mid price (if it doesn't exist)
        if 'mid' not in df.columns:
            df['mid'] = (df['bid'].values + df['ask'].values) / 2.0

        # Create synthetic bid/ask if they don't exist
        if 'bid' not in df.columns:
            df['bid'] = 0.9995 * df['mid'].values

        if 'ask' not in df.columns:
            df['ask'] = 1.0005 * df['mid'].values

        # First strip away out of hours times
        # remove any trades before 6am and after 9pm GMT
        return df