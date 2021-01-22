from __future__ import division, print_function

__author__ = 'saeedamen' # Saeed Amen / saeed@cuemacro.com

#
# Copyright 2017 Cuemacro
#
# See the License for the specific language governing permissions and limitations under the License.
#
# This may not be distributed without the permission of Cuemacro
#

from tcapy.util.colors import Colors

import os

colors = Colors()

from collections import OrderedDict

class ConstantsGen(object):
    """The constants of 'test_tcapy' version of tcapy are stored here. They govern behavior such as:

       - GUI listeners and lines
       - we can override any of these settings in constantsgencred.py or constantscred.py

    """
##### dash callbacks and web GUI lines #################################################################################

    dash_callbacks = {}

    dash_callbacks['detailed'] = ['ticker-val', 'start-date-val', 'start-time-val', 'finish-date-val',
                               'finish-time-val', 'broker-val', 'algo-val', 'venue-val', 'market-data-val',
                               'metric-val',
                               'calculation-button']

    dash_callbacks['aggregated'] = ['ticker-val', 'start-date-val', 'finish-date-val', 'broker-val', 'algo-val',
                                 'venue-val',
                                 'reload-val', 'market-data-val', 'event-type-val', 'metric-val',
                                 'calculation-button']

    dash_callbacks['compliance'] = ['ticker-val', 'start-date-val', 'finish-date-val', 'broker-val', 'algo-val', 'venue-val',
                                 'reload-val', 'market-data-val', 'filter-time-of-day-val',
                                 'start-time-of-day-val', 'finish-time-of-day-val', 'slippage-bounds-val',
                                 'visualization-val',
                                 'calculation-button']

##### Default colors/lines for the detailed timeline chart #############################################################

    detailed_timeline_plot_lines = {'mid': {'color': 'blue', 'linewidth': 3, 'chart_type': 'line'},
                                    'bid': {'color': 'black', 'linewidth': 0.5, 'chart_type': 'line'},
                                    'ask': {'color': 'black', 'linewidth': 0.5, 'chart_type': 'line'},
                                    'arrival': {'color': 'green', 'linewidth': 3, 'chart_type': 'dash',
                                                'line_shape': 'hv'},
                                    'twap': {'color': 'grey', 'linewidth': 0.5, 'chart_type': 'dash',
                                             'line_shape': 'hv'},
                                    'vwap': {'color': 'blue', 'linewidth': 0.5, 'chart_type': 'dot',
                                             'line_shape': 'hv'},
                                    'buy trade': {'color': 'green', 'linewidth': 1, 'chart_type': 'bubble'},
                                    'sell trade': {'color': 'red', 'linewidth': 1, 'chart_type': 'bubble'},
                                    'other' : {'color': ['grey', 'orange', 'red'], 'linewidth': 0.5, 'chart_type': 'dash',
                                             'line_shape': 'hv'}}

    # Max number of days lookback window for any TCA calculation versus today
    gui_lookback_window = 4*365

    # Max number of days to allow plotting on detailed calculation
    max_plot_days = 180

    gui_table_type = 'html' # 'html' or 'dash'

    plotly_webgl = False

    if plotly_webgl:
        chart_max_time_points = 1440 / 2.0
    else:
        chart_max_time_points = 1440 / 4.0

        # Max number of time points for Plotly (note: this will be multiplied by number of columns)
        # having too many points plotted, slows down plotly significantly

    # For plots
    chart_silent_display = False

    chart_default_engine = "plotly"  # which backend plotting engine should we use (only plotly supported)
    chart_source = "Web"  # label for data source
    chart_brand_label = "vis"  # brand to add the chart
    chart_display_source_label = True  # should we add the source label
    chart_display_brand_label = True  # should we add the brand label
    chart_brand_color = "#C0C0C0"

    chart_scale_factor = 1
    chart_dpi = 100

    # orca can be used to generate PNG files from Plotly charts
    orca_server_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/batch_scripts/orca"

    # Default size for Plotly charts we use
    chart_width = 980
    chart_height = 500

    # Default setting for Plotly charts we use to create reports (should generally be smaller)
    chart_report_width = 600
    chart_report_height = 400

    # Default setting for xlwings charts we use to create reports (should generally be smaller)
    # Note, depending on your version of Excel, these sizes/distances may need to be adjusted
    chart_xlwings_report_width = chart_report_width * (2/3)
    chart_xlwings_report_height = chart_report_height * (2/3)

    chart_xlwings_listpoints_row = 12
    chart_xlwings_top_row = 44
    chart_xlwings_row_height = 12.25
    chart_xlwings_vertical_gap = 75

    ########## PLOTLY SETTINGS
    plotly_world_readable = False
    plotly_sharing = 'private'
    plotly_theme = 'pearl'
    plotly_plot_mode = 'dash'  # 'dash', 'offline_jupyter', 'offline_html'
    plotly_palette = ['#E24A33',
                      '#348ABD',
                      '#988ED5',
                      '#777777',
                      '#FBC15E',
                      '#8EBA42',
                      '#FFB5B8']

    candlestick_increasing_color = 'white'
    candlestick_increasing_line_color = 'grey'
    candlestick_decreasing_color = 'blue'
    candlestick_decreasing_line_color = 'blue'

    chart_type_candlesticks = 'candlesticks'  # either 'candlesticks' or 'ohlc'

    bubble_size_scalar = 35

    # Properties for plotting timelines on their own (ie. excludes line+candlestick plots)

    # What should we do with DataFrames which have NaNs before attempting to plot?
    # eg. should we fill down NaN? Apply some interpolation of some sort? Do nothing?
    timeline_fillna = 'nofill'  # do nothing - 'nofill'
    # fill down NA values with zero - 'zero'
    # fill down NA values for plot - 'ffill',
    # interpolation - 'linear', 'time', 'index', 'values', 'nearest', 'zero',
    # 'slinear', 'quadratic', 'cubic', 'barycentric', 'krogh', 'polynomial', 'spline',
    # 'piecewise_polynomial', 'from_derivatives', 'pchip', 'akima'
    # see https://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.interpolate.html

    timeline_line_type = 'line+markers'  # 'scatter' or 'line' or 'line+markers' or 'bar'
    timeline_lineshape = 'linear'  # 'vhv', 'hvh', 'hv', 'spline', 'linear' (see https://plot.ly/python/line-charts/)
    timeline_connect_line_gaps = True

    ##### Colors for plotting
    # 'red'   :   '#E24A33',
    # 'blue'  :   '#348ABD',
    # 'purple':   '#988ED5',
    # 'gray'  :   '#777777',
    # 'yellow':   '#FBC15E',
    # 'green' :   '#8EBA42',
    # 'pink'  :   '#FFB5B8'

    chart_default_colormap = 'Blues'

    # Nicer than the default colors of matplotlib (fully editable!)
    # list of colors from http://www.github.com/santosjorge/cufflinks project
    # where I've overwritten some of the primary colours (with the above)
    chart_color_overwrites = colors.chart_color_overwrites

#### Metrics ###########################################################################################################

    ##### Markout windows (in unit of measure milliseconds, seconds or minutes)
    markout_windows = [-120, -60, -20, 0, 20, 60, 120]

    markout_unit_of_measure = 's'  # ms, s or m

    ##### Wide benchmark markout windows (as multiples of wide benchmark window)
    wide_benchmark_markout_windows_multiplier = range(-10, 10 + 1, 1)

    wide_benchmark_markout_unit = 1
    wide_benchmark_unit_of_measure = 's'  # ms, s or m

    ##### When calculating PDF and histograms weight the slippage by this field (select None if want to remove this)

    # or can use 'executed_notional' - it is preferred to use reporting currency when aggregating amongst many currency
    # pairs
    pdf_weighting_field = 'executed_notional_in_reporting_currency'
    summary_weighting_field = 'executed_notional_in_reporting_currency'
    date_weighting_field = 'executed_notional_in_reporting_currency'
    table_weighting_field = 'executed_notional_in_reporting_currency'