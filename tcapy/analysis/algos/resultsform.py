from __future__ import print_function, division

__author__ = 'saeedamen'  # Saeed Amen / saeed@cuemacro.com

#
# Copyright 2017 Cuemacro Ltd. - http//www.cuemacro.com / @cuemacro
#
# See the License for the specific language governing permissions and limitations under the License.
#

import abc
import pandas as pd

from tcapy.analysis.algos.resultssummary import ResultsSummary

from tcapy.conf.constants import Constants
from tcapy.analysis.tradeorderfilter import TradeOrderFilterTag
from tcapy.util.utilfunc import UtilFunc
from tcapy.util.timeseries import TimeSeriesOps

constants = Constants()

# compatible with Python 2 *and* 3:
ABC = abc.ABCMeta('ABC', (object,), {'__slots__': ()})

class ResultsForm(ABC):
    """Takes in trade and then creates aggregated metrics which can be displayed in multiple forms by its
    implementations (such as tables, bar charts, distributions, scatter charts etc). Note, this does not create the charts
    themselves, more preparing the DataFrames for later plotting

    """
    def __init__(self, trade_order_list=None, metric_name=None, aggregate_by_field=None, aggregation_metric='mean',
                 tag_value_combinations={}):
        self._trade_order_list = trade_order_list
        self._metric_name = metric_name
        self._aggregate_by_field = aggregate_by_field
        self._aggregation_metric = aggregation_metric
        self._results_summary = ResultsSummary()

        self._tag_value_combinations = tag_value_combinations
        self._trade_order_filter_tag = TradeOrderFilterTag()
        self._util_func = UtilFunc()
        self._time_series_ops = TimeSeriesOps()

    def _check_calculate_results(self, trade_order_name):
        if trade_order_name is not None and self._trade_order_list is not None:
            if trade_order_name not in self._trade_order_list:
                return False

        return True

    @abc.abstractmethod
    def aggregate_results(self, trade_order_df=None, market_df=None, trade_order_name=None, metric_name=None,
                          ticker=None, aggregate_by_field=None, filter_nan=True):
        pass

class TableResultsForm(ResultsForm):
    """Takes in trade/orders and then creates aggregated metrics which are likely to be displayed as a table. Can also
    sort by best/worst metrics, rounding numbers etc.

    """
    def __init__(self, trade_order_list=None, metric_name=None, filter_by=['all'], tag_value_combinations={},
                 keep_fields=['executed_notional', 'side'], replace_text={}, round_figures_by=1, scalar=1.0,
                 weighting_field=constants.table_weighting_field, exclude_fields_from_avg=[], remove_index=False):
        self._trade_order_list = trade_order_list
        self._metric_name = metric_name
        self._results_summary = ResultsSummary()
        self._keep_fields = keep_fields
        self._filter_by = filter_by
        self._replace_text = replace_text
        self._round_figures_by = round_figures_by
        self._weighting_field = weighting_field
        self._scalar = scalar
        self._exclude_fields_from_avg = exclude_fields_from_avg
        self._remove_index = remove_index

        self._tag_value_combinations = tag_value_combinations
        self._trade_order_filter_tag = TradeOrderFilterTag()
        self._results_form_tag = 'table'
        self._util_func = UtilFunc()
        self._time_series_ops = TimeSeriesOps()

    def aggregate_results(self, trade_order_df=None, market_df=None, filter_by=[], trade_order_name=None,
                          metric_name=None, ticker=None,
                          filter_nan=True,
                          weighting_field=None,
                          tag_value_combinations={}, keep_fields=[], remove_fields=[], replace_text={},
                          round_figures_by=None, scalar=None,
                          exclude_fields_from_avg=None, remove_index=False):
        if not (self._check_calculate_results(trade_order_name)): return [None, None]

        if metric_name is None: metric_name = self._metric_name
        if keep_fields == []: keep_fields = self._keep_fields
        if filter_by == []: filter_by = self._filter_by
        if round_figures_by is None: round_figures_by = self._round_figures_by
        if replace_text == {}: replace_text = self._replace_text
        if weighting_field is None: weighting_field = self._weighting_field
        if tag_value_combinations == {}: tag_value_combinations = self._tag_value_combinations
        if scalar is None: scalar = self._scalar
        if exclude_fields_from_avg is None: exclude_fields_from_avg = self._exclude_fields_from_avg
        if remove_index is None: remove_index = self._remove_index

        if not (isinstance(metric_name, list)): metric_name = [metric_name]
        if not (isinstance(filter_by, list)): filter_by = [filter_by]

        trade_order_df = self._trade_order_filter_tag.filter_trade_order(trade_order_df,
                                                                         tag_value_combinations=tag_value_combinations)

        results = []

        for filt in filter_by:
            for met in metric_name:
                if met not in trade_order_df.columns:
                    results.append(None)

                elif weighting_field is not None and weighting_field not in trade_order_df.columns:
                    results.append(None)

                else:
                    metric_fields_to_filter = [x for x in trade_order_df.columns if met in x]

                    columns_to_keep = self._util_func.flatten_list_of_lists([keep_fields, metric_fields_to_filter])

                    results_df = trade_order_df[columns_to_keep]

                    # Apply filter
                    if 'worst' in filt:
                        ordinal = filt.split('worst_')[1]

                        results_df = results_df.sort_values(by=met, ascending=True)

                        if ordinal != 'all':
                            results_df = results_df.head(int(ordinal))

                    elif 'best' in filt:
                        ordinal = filt.split('worst_')[1]
                        results_df = results_df.sort_values(by=met, ascending=False)

                        if ordinal != 'all':
                            results_df = results_df.head(ordinal)

                    # Weighting field for average!
                    results_df = self._time_series_ops.weighted_average_of_each_column(results_df, weighting_field,
                                                                                       append=True,
                                                                                       exclude_fields_from_avg=exclude_fields_from_avg)

                    results_df = self._time_series_ops.multiply_scalar_dataframe(results_df, scalar=scalar)
                    results_df = self._time_series_ops.round_dataframe(results_df, round_figures_by,
                                                                       columns_to_keep=columns_to_keep)

                    results_df = self._util_func.replace_text_in_cols(results_df, replace_text)

                    if remove_index:
                        results_df = results_df.reset_index()
                        results_df = results_df.set_index(results_df.columns[0])

                    results.append(
                        (results_df, self._results_form_tag + '_' + trade_order_name + '_' + met + '_by_' + filt))

        return results

class ScatterResultsForm(ResultsForm):
    """Takes in trade/orders and then creates aggregated metrics which are likely to be displayed as a scatter plot.

    """
    def __init__(self, trade_order_list=None, filter_by=['all'], tag_value_combinations={},
                 scatter_fields=['executed_notional', 'slippage'], replace_text={}, round_figures_by=1, scalar=1.0):
        self._trade_order_list = trade_order_list
        self._results_summary = ResultsSummary()
        self._scatter_fields = scatter_fields
        self._filter_by = filter_by
        self._replace_text = replace_text
        self._round_figures_by = round_figures_by
        self._scalar = scalar

        self._tag_value_combinations = tag_value_combinations
        self._trade_order_filter_tag = TradeOrderFilterTag()
        self._results_form_tag = 'scatter'
        self._util_func = UtilFunc()
        self._time_series_ops = TimeSeriesOps()

    def aggregate_results(self, trade_order_df=None, market_df=None, trade_order_name=None,
                          scatter_fields=None,
                          tag_value_combinations={}, replace_text={},
                          round_figures_by=None, scalar=None):
        if not (self._check_calculate_results(trade_order_name)): return [None, None]

        if scatter_fields is None: scatter_fields = self._scatter_fields
        if round_figures_by is None: round_figures_by = self._round_figures_by
        if replace_text == {}: replace_text = self._replace_text
        if tag_value_combinations == {}: tag_value_combinations = self._tag_value_combinations
        if scalar is None: scalar = self._scalar

        trade_order_df = self._trade_order_filter_tag.filter_trade_order(trade_order_df,
                                                                         tag_value_combinations=tag_value_combinations)

        results = []

        if trade_order_df is not None:
            for s in scatter_fields:
                if s not in trade_order_df.columns:
                    return [None]

        else:
            return [None]

        results_df = pd.DataFrame(index=trade_order_df[scatter_fields[0]].values, data=trade_order_df[scatter_fields[1:]].values, columns=scatter_fields[1:])
        results_df.index.name = scatter_fields[0]

        results_df = self._time_series_ops.multiply_scalar_dataframe(results_df, scalar=scalar)
        results_df = self._time_series_ops.round_dataframe(results_df, round_figures_by)

        results_df = self._util_func.replace_text_in_cols(results_df, replace_text)

        results.append((results_df, self._results_form_tag + '_' + trade_order_name + '_' + "_vs_".join(scatter_fields)))

        return results

class DistResultsForm(ResultsForm):
    """Takes in trade/orders and then creates aggregated metrics which are liked to be displayed as a distribution

    """

    def __init__(self, trade_order_list=None, metric_name=None, aggregate_by_field=None, aggregation_metric='mean',
                 tag_value_combinations={}, weighting_field=constants.pdf_weighting_field, scalar=1.0):
        super(DistResultsForm, self).__init__(trade_order_list=trade_order_list, metric_name=metric_name,
                                              aggregate_by_field=aggregate_by_field,
                                              aggregation_metric=aggregation_metric,
                                              tag_value_combinations=tag_value_combinations)

        self._weighting_field = weighting_field
        self._scalar = scalar

    def aggregate_results(self, trade_order_df=None, market_df=None, trade_order_name=None, metric_name=None,
                          ticker=None, aggregate_by_field=None,
                          weighting_field=None, tag_value_combinations={}, filter_nan=True, scalar=None):
        if not (self._check_calculate_results(trade_order_name)): return [None, None]

        if metric_name is None: metric_name = self._metric_name
        if aggregate_by_field is None: aggregate_by_field = self._aggregate_by_field
        if weighting_field is None: weighting_field = self._weighting_field
        if scalar is None: scalar = self._scalar

        trade_order_df = self._trade_order_filter_tag.filter_trade_order(trade_order_df,
                                                                         tag_value_combinations=tag_value_combinations)

        if metric_name not in trade_order_df.columns: return [None]

        if weighting_field is not None and weighting_field not in trade_order_df.columns: return [None]

        if filter_nan:
            trade_order_df = trade_order_df.dropna(subset=[metric_name])

        if not (isinstance(aggregate_by_field, list)): aggregate_by_field = [aggregate_by_field]
        if not (isinstance(metric_name, list)): metric_name = [metric_name]

        results = []

        # Go through all the fields we want to aggregate by (and all the metrics)
        for agg in aggregate_by_field:
            for met in metric_name:
                results_df = None

                if trade_order_df is not None:
                    results_df = self._results_summary.field_distribution(
                        trade_order_df, market_df=market_df, postfix_label=ticker, pdf_only=True,
                        weighting_field=weighting_field, aggregate_by_field=agg, metric_name=met, scalar=scalar)

                if agg is None: agg = 'all'

                results.append((results_df, 'dist_' + trade_order_name + '_' + met + '_by_' + str(agg)))

        return results


class BarResultsForm(ResultsForm):
    """Takes in trade/orders and then creates aggregated metrics which are liked to be displayed as a bar chart

    """

    def __init__(self, trade_order_list=None, metric_name=None, aggregate_by_field=None, aggregation_metric='mean',
                 tag_value_combinations={}, scalar=1.0, round_figures_by=None,
                 weighting_field=constants.pdf_weighting_field):
        super(BarResultsForm, self).__init__(trade_order_list=trade_order_list, metric_name=metric_name,
                                             aggregate_by_field=aggregate_by_field,
                                             aggregation_metric=aggregation_metric,
                                             tag_value_combinations=tag_value_combinations)

        self._by_date = None
        self._results_form_tag = 'bar'
        self._scalar = scalar
        self._round_figures_by = round_figures_by
        self._weighting_field = weighting_field

    def aggregate_results(self, trade_order_df=None, market_df=None, trade_order_name=None, metric_name=None,
                          ticker=None, aggregate_by_field=None,
                          weighting_field=None, tag_value_combinations={}, filter_nan=True, scalar=None,
                          round_figures_by=None, aggregation_metric=None):
        if not (self._check_calculate_results(trade_order_name)): return [None, None]

        if metric_name is None: metric_name = self._metric_name
        if aggregate_by_field is None: aggregate_by_field = self._aggregate_by_field
        if scalar is None: scalar = self._scalar
        if round_figures_by is None: round_figures_by = self._round_figures_by
        if weighting_field is None: weighting_field = self._weighting_field
        if aggregation_metric is None: aggregation_metric = self._aggregation_metric

        trade_order_df = self._trade_order_filter_tag.filter_trade_order(trade_order_df,
                                                                         tag_value_combinations=tag_value_combinations)

        if metric_name not in trade_order_df.columns: return [None]

        if weighting_field is not None and weighting_field not in trade_order_df.columns: return [None]

        if filter_nan:
            trade_order_df = trade_order_df.dropna(subset=[metric_name])

        if not (isinstance(aggregate_by_field, list)): aggregate_by_field = [aggregate_by_field]
        if not (isinstance(metric_name, list)): metric_name = [metric_name]

        results = []

        # Go through all the fields we want to aggregate by (and all the metrics)
        for agg in aggregate_by_field:
            for met in metric_name:
                results_df = self._results_summary.field_bucketing(trade_order_df, metric_name=met,
                                                                   aggregate_by_field=agg,
                                                                   aggregation_metric=aggregation_metric,
                                                                   weighting_field=weighting_field,
                                                                   by_date=self._by_date) * scalar

                results_df = self._time_series_ops.round_dataframe(results_df, round_figures_by)

                if agg is None: agg = 'all'

                results_df  = self._rename_columns(results_df, met)

                results.append(
                    (results_df, self._results_form_tag + '_' + trade_order_name + '_' + met + '_by_' + str(agg)))

        return results

    def _rename_columns(self, df, index_name):
        df.columns = [str(r) + '' for r in df.columns]
        df.index.name = df.index.name + '_index'

        return df

class TimelineResultsForm(BarResultsForm):
    """Takes in trade/orders and then creates aggregated metrics which are liked to be displayed as a timeline

    """
    def __init__(self, trade_order_list=None, metric_name=None, aggregate_by_field=None, aggregation_metric='mean',
                 tag_value_combinations={}, by_date='date', round_figures_by=None,
                 weighting_field=constants.pdf_weighting_field, scalar=10000.0):
        super(TimelineResultsForm, self).__init__(trade_order_list=trade_order_list, metric_name=metric_name,
                                                  aggregate_by_field=aggregate_by_field,
                                                  aggregation_metric=aggregation_metric,
                                                  tag_value_combinations=tag_value_combinations,
                                                  round_figures_by=round_figures_by,
                                                  scalar=scalar)

        self._by_date = by_date
        self._results_form_tag = 'timeline'
        self._weighting_field = weighting_field

    def _rename_columns(self, df, index_name):
        df.columns = [str(r) + '' for r in df.columns]
        df.index.name = 'Date'

        return df


class JoinTables(object):
    """Takes in DataFrames which are joined together according to user preferences

    """

    def __init__(self, tables_dict={}, scalar=1, round_figures_by=None):
        self._tables_dict = tables_dict
        self._scalar = scalar
        self._round_figures_by = round_figures_by

        self._time_series_ops = TimeSeriesOps()
        self._util_func = UtilFunc()

    def aggregate_tables(self, df_dict={}, tables_dict={}, round_figures_by=None, scalar=None):
        if tables_dict == {}: tables_dict = self._tables_dict
        if round_figures_by is None: round_figures_by = self._round_figures_by
        if scalar is None: scalar = self._scalar

        joined_results = []

        table_name = tables_dict['table_name']
        table_list = tables_dict['table_list']

        column_list = None; replace_text = None

        if 'column_list' in tables_dict.keys():
            column_list = tables_dict['column_list']

        if 'replace_text' in tables_dict.keys():
            replace_text = tables_dict['replace_text']

        agg_results = []

        for i in range(0, len(table_list)):
            table = table_list[i]

            if table in df_dict.keys():
                df = df_dict[table].copy()

                if column_list is not None and column_list != []:
                    df.columns = [x + ' ' + column_list[i] for x in df.columns]

                df = self._util_func.replace_text_in_cols(df, replace_text)

                if df is not None:
                    df = self._time_series_ops.multiply_scalar_dataframe(df, scalar=scalar)
                    df = self._time_series_ops.round_dataframe(df, round_figures_by)

                    agg_results.append(df)

        if agg_results != []:
            if len(agg_results) > 1:
                df_joined = self._time_series_ops.outer_join(agg_results)
            else:
                df_joined = agg_results[0]

            joined_results.append((df_joined, table_name))

        return joined_results
