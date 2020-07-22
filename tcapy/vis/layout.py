from __future__ import division, print_function

__author__ = 'saeedamen'  # Saeed Amen / saeed@cuemacro.com

#
# Copyright 2017 Cuemacro Ltd. - http//www.cuemacro.com / @cuemacro
#
# See the License for the specific language governing permissions and limitations under the License.
#

import abc

## web server components
import dash_core_components as dcc
import dash_html_components as html

import dash_table as dt

## time/date components
import datetime
from datetime import timedelta

from collections import OrderedDict
from tcapy.util.utilfunc import UtilFunc

from tcapy.conf.constants import Constants

constants = Constants()

# compatible with Python 2 *and* 3:
ABC = abc.ABCMeta('ABC', (object,), {'__slots__': ()})


class Layout(ABC):
    """Abstract class for creating HTML pages via Dash/HTML components. Has generic methods for creating HTML/Dash
    components, including, header bars, link bars, buttons and plots
    """

    def __init__(self, url_prefix=''):
        self.id_flags = {}
        self.pages = {}

        self._util_func = UtilFunc()
        self._url_prefix = url_prefix

    def id_flag_parameters(self):
        return self.id_flags

    def flatten_list_of_strings(self, list_of_lists):
        """Flattens lists of strings, into a single list of strings (rather than characters, which is default behavior).

        Parameters
        ----------
        list_of_lists : str (list)
            List to be flattened

        Returns
        -------
        str (list)
        """

        rt = []
        for i in list_of_lists:
            if isinstance(i, list):
                rt.extend(self.flatten_list_of_strings(i))
            else:
                rt.append(i)
        return rt

    def header_bar(self, title):
        """Creates HTML for the header bar

        Parameters
        ----------
        title : str
            Title of the header

        Returns
        -------
        html.Div
        """

        img = ''

        try:
            img = self.encoded_image.decode()
        except:
            pass

        return html.Div([
            html.H1(title, className='eight columns'),
            html.Img(
                src='data:image/png;base64,{}'.format(img),
                # className='one columns',
                style={
                    'height': '100px',
                    'width': '100px',
                    'float': 'right',
                    # 'position': 'relative',
                },
            ),

        ],
            style={'width': '1000px', 'marginBottom': 0, 'marginTop': 5, 'marginLeft': 5,
                   'marginRight': 5})

    def button(self, caption=None, id=None, prefix_id='', className=None, upload=False):
        """Creates an HTML button

        Parameters
        ----------
        caption : str (default: None)
            Caption for the HTML object

        id : str (default: None)
            ID for the HTML object

        prefix_id : str (default:'')
            Prefix to use for the ID

        className: str (default: None)
            CSS class to use for formatting

        upload : bool
            Is this an upload button?

        Returns
        -------
        html.Div
        """

        if prefix_id != '':
            id = prefix_id + '-' + id

        if className is None:
            button = html.Button(caption, id=id, n_clicks=0)

            if upload:
                button = dcc.Upload(button)

            return html.Div([
                button
            ], style={'width': '150px', 'display': 'inline-block', 'marginBottom': 0, 'marginTop': 0, 'marginLeft': 5,
                      'marginRight': 5})

        else:
            button = html.Button(caption, id=id, n_clicks=0, className=className)

            if upload:
                button = dcc.Upload(button)

            return html.Div([
                button, " "
            ], style={'width': '800px', 'display': 'inline-block', 'marginBottom': 0, 'marginTop': 0, 'marginLeft': 5,
                      'marginRight': 5})

    def uploadbox(self, caption=None, id=None, prefix_id='', className=None):
        """Creates an HTML button

        Parameters
        ----------
        caption : str (default: None)
            Caption for the HTML object

        id : str (default: None)
            ID for the HTML object

        prefix_id : str (default:'')
            Prefix to use for the ID

        className: str (default: None)
            CSS class to use for formatting

        upload : bool
            Is this an upload button?

        Returns
        -------
        html.Div
        """

        if prefix_id != '':
            id = prefix_id + '-' + id

        area = dcc.Upload(id=id, children=html.Div([caption + ': Drag and Drop or ', html.A('Select Files')],
                                                   style={'borderWidth': '1px', 'width' : '980px', 'borderStyle': 'dashed', 'borderRadius': '5px'}))

        if className is None:

            return html.Div([
                area
            ], style={'width': '980px', 'display': 'inline-block', 'marginBottom': 0, 'marginTop': 0, 'marginLeft': 5,
                      'marginRight': 5})

        else:
            area = dcc.Upload(id=id, children=html.Div(['Drag and Drop or ', html.A('Select Files')]))

            return html.Div([
                area, " "
            ], style={'width': '980px', 'display': 'inline-block', 'marginBottom': 0, 'marginTop': 0, 'marginLeft': 5,
                      'marginRight': 5})

    def plot(self, caption=None, id=None, prefix_id='', element_add=None, downloadplot_caption=None,
             downloadplot_tag=None, download_file=None):
        """Creates a Plotly plot object (Dash component)

        Parameters
        ----------
        caption : str (default: None)
            Caption for the HTML object

        id : str (default: None)
            ID for the HTML object

        prefix_id : str (default:'')
            Prefix to use for the ID

        element_add : HTML component (default: None)
            Add this HTML component at the start

        downloadplot_caption : str (default: None)
            Caption for the download CSV

        downloadplot_tag : str (default: None)
            Tag for the download plot object

        download_file : str
            Download file name

        Returns
        -------
        html.Div
        """

        if prefix_id != '':
            prefix_id = prefix_id + '-'

        html_tags = []
        html_tags.append(html.H3(caption))

        if element_add is not None:
            html_tags.append(element_add)

        if isinstance(id, str):
            id = [id]

        # config={'editable': True, 'modeBarButtonsToRemove': ['sendDataToCloud']

        for id_ in id:
            html_tags.append(html.Div([
                dcc.Graph(id=prefix_id + id_, style={'width': '1000px', 'height': '500px'})
                # , config={'modeBarButtonsToRemove': ['sendDataToCloud']})
            ]))

        html_style = {'width': '1000px', 'marginBottom': 0, 'marginTop': 0, 'marginLeft': 5,
                      'marginRight': 5}

        html_tags = self.download_file_link(html_tags, prefix_id, downloadplot_caption, downloadplot_tag, download_file)

        return html.Div(html_tags, style=html_style)

    def download_file_link(self, html_tags, prefix_id, downloadplot_caption_list, downloadplot_tag_list, download_file_list):
        """Creates links for downloading CSV files (typically associated with plots and tables)

        Parameters
        ----------
        html_tags : list
            List for the HTML tags to be appended to

        prefix_id : str
            Prefix ID with this

        downloadplot_caption_list : str (list)
            List of captions for each download

        downloadplot_tag_list : str (list)
            List of IDs for the tags

        download_file_list : str (list)
            Download file list

        Returns
        -------
        html.Div (list)
        """

        if html_tags is None:
            html_tags = []

        if downloadplot_caption_list != None and downloadplot_tag_list != None and download_file_list != None:

            if not(isinstance(downloadplot_caption_list, list)):
                downloadplot_caption_list = [downloadplot_caption_list]

            if not(isinstance(downloadplot_tag_list, list)):
                downloadplot_tag_list = [downloadplot_tag_list]

            if not(isinstance(download_file_list, list)):
                download_file_list = [download_file_list]

            for i in range(0, len(download_file_list)):

                html_download = html.Div([
                    html.A(
                        downloadplot_caption_list[i],
                        id=prefix_id + downloadplot_tag_list[i],
                        download=download_file_list[i],
                        href="",
                        target="_blank"
                    ),
                ], style={'width': '300px', 'display': 'inline-block', 'marginBottom': 0, 'marginTop': 0, 'marginLeft': 5,
                          'marginRight': 5, 'className': 'row'})

                html_tags.append(html_download)

        return html_tags


    def table(self, caption=None, id=None, prefix_id='', element_add=None, columns=None, downloadplot_caption=None,
             downloadplot_tag=None, download_file=None):
        """

        Parameters
        ----------
        caption : str (default: None)
            Caption for the HTML object

        id : str (default: None)
            ID for the HTML object

        prefix_id : str (default:'')
            Prefix to use for the ID

        element_add : HTML component (default: None)
            Add this HTML component at the start

        columns : str (list)
            Column headers

        downloadplot_caption : str (default: None)
            Caption for the download CSV

        downloadplot_tag : str (default: None)
            Tag for the download plot object

        download_file : str
            Download file name

        Returns
        -------
        html.Div
        """

        if prefix_id != '':
            prefix_id = prefix_id + '-'

        html_tags = []
        html_tags.append(html.H3(caption))

        if element_add is not None:
            html_tags.append(element_add)

        if isinstance(id, str):
            id = [id]

        for i in range(0, len(id)):

            id_ = id[i]

            if i == len(id) - 1:
                line_break = None
            else:
                line_break = html.Br()

            if columns is None:

                if constants.gui_table_type == 'dash':
                    data_table = dt.DataTable(
                        # data=[{}],
                        #row_selectable='single',
                        # columns=[{"name": [], "id": []}],
                        filtering=True,
                        sorting=True,
                        selected_rows=[],
                        id=prefix_id + id_
                    )
                else:
                    data_table = html.Div([
                        html.Div(id=prefix_id + id_)
                        # , config={'modeBarButtonsToRemove': ['sendDataToCloud']})
                    ])
            else:
                col = columns

                if isinstance(columns, dict):
                    col = columns[id_]

                if constants.gui_table_type == 'dash':
                    data_table = dt.DataTable(
                        # data=[{}],
                        #row_selectable='single',
                        # columns=[{"name": i, "id": i} for i in col],
                        filtering=True,
                        sorting=True,
                        selected_rows=[],
                        id=prefix_id + id_
                    )
                else:
                    data_table = html.Div([
                        html.Div(id=prefix_id + id_)
                        # , config={'modeBarButtonsToRemove': ['sendDataToCloud']})
                    ])

            html_tags.append(html.Div([
                # html.Div(id=prefix_id + id_)
                data_table,
                line_break

                # , config={'modeBarButtonsToRemove': ['sendDataToCloud']})
            ]))

        html_tags = self.download_file_link(html_tags, prefix_id, downloadplot_caption, downloadplot_tag, download_file)

        html_style = {'width': '1000px', 'display': 'inline-block', 'marginBottom': 5, 'marginTop': 5, 'marginLeft': 5,
                      'marginRight': 5}

        return html.Div(html_tags, style=html_style)

    def horizontal_bar(self):
        """A horizontal HTML bar

        Returns
        -------
        html.Div
        """
        # horizonal bar
        return self.width_cel(html.Hr())

    def width_cel(self, html_obj, margin_left=0):
        """Wraps around an HTML object to create a wide table

        Parameters
        ----------
        html_obj : HTML
            HTML object to be wrapped around

        margin_left : int (default: 0)
            Margin of HTML

        Returns
        -------
        html.Div
        """
        # create a whole width table cell

        return html.Div([
            html_obj
        ],
            style={'width': '1000px', 'display': 'inline-block', 'marginBottom': 5, 'marginTop': 5,
                   'marginLeft': margin_left,
                   'marginRight': 0, 'className': 'row'})

    def link_bar(self, labels_links_dict, add=None):
        """Creates an link bar of Dash components, typically used as a menu on the top of a Dash based web page.

        Parameters
        ----------
        labels_links_dict : dict
            Dictionary containing labels and links to be used

        add : HTML (default: None)
            HTML object to be appended

        Returns
        -------
        html.Div
        """

        # creates a link bar
        key_list = self._util_func.dict_key_list(labels_links_dict.keys())

        if self._url_prefix == '':
            url_prefix = '/'
        else:
            url_prefix = '/' + self._url_prefix + '/'


        if len(labels_links_dict) == 1:
            list = [dcc.Link(key_list[0], href=url_prefix)]

        elif len(labels_links_dict) == 2:
            list = [dcc.Link(key_list[0], href=url_prefix + labels_links_dict[key_list[0]]), ' / ',
                    dcc.Link(key_list[1], href=url_prefix + labels_links_dict[key_list[1]])]
        else:
            list = [dcc.Link(key_list[0], href=url_prefix), ' / ', ]

            for i in range(1, len(labels_links_dict) - 1):
                list.append(dcc.Link(key_list[i], href=url_prefix + labels_links_dict[key_list[i]]))
                list.append(' / ')

            list.append(list.append(
                dcc.Link(key_list[-1], href=url_prefix + labels_links_dict[key_list[-1]])))

        if add is not None:
            list.append(add)

        return html.Div(list,
                        style={'width': '800px', 'display': 'inline-block', 'marginBottom': 5, 'marginTop': 5,
                               'marginLeft': 5,
                               'marginRight': 5, 'className': 'row'})

    def drop_down(self, caption=None, id=None, prefix_id='', drop_down_values=None, multiselect=False, width=155,
                  multiselect_start_values=None):
        """Creates a Dash drop down object, wrapped in HTML table

        Parameters
        ----------
        caption : str (default: None)
            Caption for the HTML object

        id : str (list) (default: None)
            ID for the HTML object

        prefix_id : str (default:'')
            Prefix to use for the ID

        drop_down_values : str (list) (default: None)
            List of drop down values

        multiselect : bool (default: False)
            Can we select multiple values?

        width : int (default: 155)
            Width of the object to display

        multiselect_start_values : str (default: None)
            Which elements to select at the start

        Returns
        -------
        html.Div
        """
        # creates drop down style HTML controls
        if prefix_id != '':
            prefix_id = prefix_id + '-'

        drop_list = []

        # for each ID assign the drop down values
        if isinstance(id, str):
            id = {id: drop_down_values}

        elif isinstance(id, list):
            id_list = id

            id = OrderedDict()

            for i in id_list:
                id[i] = drop_down_values

        if caption is not None:
            drop_list = [html.P(caption)]

        # for each ID create a drop down object
        for key in self._util_func.dict_key_list(id.keys()):

            if multiselect_start_values is None:
                start_values = id[key][0]
            else:
                start_values = multiselect_start_values

            # each drop down as the same drop down values
            drop_list.append(dcc.Dropdown(
                id=prefix_id + key,
                options=[{'label': j, 'value': j} for j in id[key]],
                value=start_values,
                multi=multiselect
            ))

        # wrap it into an HTML Div style table
        return html.Div(drop_list,
                        style={'width': str(width) + 'px', 'display': 'inline-block', 'marginBottom': 0, 'marginTop': 0,
                               'marginLeft': 5,
                               'marginRight': 5})



    def timeline_dropdown(self, prefix, available_plot_lines):
        """Create a dropdown for timelines (with multiple selectable values)

        Parameters
        ----------
        prefix : str

        available_plot_lines : str (list)

        Returns
        -------
        html.Div
        """
        return html.Div([
            self.drop_down(caption=None, id=prefix + '-lines-val',
                           drop_down_values=available_plot_lines,
                           multiselect=True, multiselect_start_values=available_plot_lines, width=975)
        ])

    @abc.abstractmethod
    def create_layouts(self):
        """Create the final page layout, which is likely a collection of various HTML objects

        Returns
        -------

        """
        pass

    def date_picker(self, caption=None, id=None, prefix_id='', initial_date=datetime.date.today(), offset=None, width=155):

        if isinstance(id, str):
            id = [id]

        date_picker_list = [html.P(caption)]

        if prefix_id != '':
            prefix_id = prefix_id + '-'

        for i in range(0, len(id)):

            id_ = id[i]

            offset_ = 0
            if offset is not None:
                offset_ = offset[i]

            # date_picker_list.append(dcc.Input(
            #     id=prefix_id + id_,
            #     type='date',
            #     value=datetime.date.today() - datetime.timedelta(days=60)
            # ))

            date_picker_list.append(html.Div(children=dcc.DatePickerSingle(
                id=prefix_id + id_,
                min_date_allowed=datetime.date.today() - datetime.timedelta(days=365*3),
                max_date_allowed=datetime.date.today(),
                date=initial_date + datetime.timedelta(days=offset_),
                display_format='DD/MM/YY'), style={'padding': 5, 'height' : 5, 'font-size' : '24px !important'},
            ))

            #if i < len(id) - 1:
            #    date_picker_list.append(' to ')

        return html.Div(date_picker_list,
                        style={'width': str(width) + 'px', 'display': 'inline-block', 'marginBottom': 0, 'marginTop': 0,
                               'marginLeft': 5,
                               'marginRight': 5})
    #
    # style = {'width': str(width) + 'px', 'display': 'inline-block', 'marginBottom': 0, 'marginTop': 0,
    #          'marginLeft': 5,
    #          'marginRight': 5}

    def date_picker_range(self, caption=None, id=None, prefix_id='', initial_date=datetime.date.today(), offset=[-7, -1]):

        date_picker_list = []
        date_picker_list.append(caption + '      ')

        if prefix_id != '':
            prefix_id = prefix_id + '-'

        date_picker_list.append(dcc.DatePickerRange(
            id=prefix_id + id,
            min_date_allowed=datetime.date.today() - datetime.timedelta(days=120),
            max_date_allowed=datetime.date.today(),
            start_date=initial_date + timedelta(days=offset[0]),
            end_date_placeholder_text="Pick a date",
            display_format='DD/MM/YY'
        ))

        return html.Div(date_picker_list,
                        style={'width': '600px', 'display': 'inline-block', 'marginBottom': 0, 'marginTop': 0,
                               'marginLeft': 5,
                               'marginRight': 5})


