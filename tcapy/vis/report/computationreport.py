from __future__ import print_function

__author__ = 'saeedamen'  # Saeed Amen / saeed@cuemacro.com

#
# Copyright 2018 Cuemacro Ltd. - http//www.cuemacro.com / @cuemacro
#
# See the License for the specific language governing permissions and limitations under the License.
#

import abc
import datetime

from chartpy import Canvas, Chart, Style

from tcapy.util.utilfunc import UtilFunc

# compatible with Python 2 *and* 3:
ABC = abc.ABCMeta('ABC', (object,), {'__slots__': ()})

class ComputationReport(ABC):
    """Converts ComputationResults (largely consisting of Plotly based Figures and HTML tables) into self contained HTML pages.
    Can also render these HTML pages into PDFs. Uses chartpy's "Canvas" object extensively

    """

    def __init__(self, computation_results, title='Cuemacro Computation'):
        """Initialize class, with the computation results we wish to convert into a report like format

        Parameters
        ----------
        computation_results : ComputationResults
            The results of a large scale computation, which contains charts and DataFrames

        title : str
            Title of webpage to be rendered
        """
        self._util_func = UtilFunc()
        self._computation_results = computation_results
        self._title = title
        self._canvas_plotter = 'plain'
        self._chart = Chart(engine='plotly')

    def create_report(self, output_filename=None, output_format='html', offline_js=False):
        """Creates an HTML/PDF report from a ComputationResult object, which can (optionally) be written to disk, alternatively
        returns a binary representation of the HTML or PDF.

        Parameters
        ----------
        output_filename : str (optional)
            File output, if this is not specified a binary object is returned

        output_format : str
            'html' (default) - output an HTML page

        offline_js : bool
            False (default) - download's Plotly.js in webpage to be rendered
            True - includes Plotly.js in web page to be rendered

        Returns
        -------
        pdf or HTML binary

        """

        # extra code to put in the <head> part of HTML
        extra_head_code = ''

        if output_format == 'html':

            # embed plotly.js in HTML (makes it bigger, but then doesn't require web connection)
            if offline_js:
                embed_chart = 'offline_embed_js_div'
            else:
            # otherwise put web link to plotly.js (but this means we need to download every time)
                embed_chart = 'offline_div'
                extra_head_code = '<head><script src="https://cdn.plot.ly/plotly-latest.min.js"></script></head>'
        elif output_format == 'pdf':
            # for PDFs we need to create static PNGs of plotly charts
            embed_chart = 'offline_image_png_in_html'

        # get a list of the HTML to render
        elements_to_render = self._layout_computation_results_to_html(embed_chart)
        canvas = Canvas(elements_to_render=elements_to_render)

        # should we return a binary string containing the HTML/PDF (this can be displayed by a web server for example)
        # or later be written to disk
        return_binary = False

        # return a binary string, if we haven't specified a filename output
        if output_filename is None:
            return_binary = True

        # generate the HTML or PDF with chartpy's Canvas object
        if output_format == 'html':

            html, _ = canvas.generate_canvas(output_filename=output_filename, silent_display=True,
                canvas_plotter=self._canvas_plotter, page_title=self._title, render_pdf=False,
                return_html_binary=return_binary, extra_head_code=extra_head_code)

            return html
        elif output_format == 'pdf':
            _, pdf = canvas.generate_canvas(output_filename=output_filename, silent_display=True,
                canvas_plotter=self._canvas_plotter, page_title=self._title, render_pdf=True,
                return_pdf_binary=return_binary, extra_head_code=extra_head_code)

            return pdf
        else:
            raise Exception("Invalid output format selected")

    def _generate_filename(self, extension):
        return (self._get_time_stamp() + "." + extension)

    def _get_time_stamp(self):
        return str(datetime.datetime.now()).replace(':', '-').replace(' ', '-').replace(".", "-")

    def _create_text_html(self, text):
        """Takes text and then creates the appropriate HTML to represent it, split by horizontal HTML bars

        Parameters
        ----------
        text : str (list)
            Text to be added in HTML

        Returns
        -------
        list (of HTML)
        """
        if text != [] and text is not None:
            html_output =[['<hr>']]
        else:
            html_output = []

        if not(isinstance(text, list)):
            text = [text]

        for t in text:
            html_output.append([t])

        return html_output

    def _create_table_html(self, table):
        """Takes tables in HTML and then creates the appropriate HTML to represent it, split by horizontal HTML bars

        Parameters
        ----------
        text : str (list)
            Tables in HTML format

        Returns
        -------
        list (of HTML)
        """
        if table != {} and table is not None:
            html_output = [['<hr>']]
        else:
            html_output = []

        for t in self._util_func.dict_key_list(table.keys()):
            html_output.append(table[t])

        return html_output

    def _create_chart_html(self, chart, embed_chart):
        if chart != {} and chart is not None:
            html_output = [['<hr>']]
        else:
            html_output = []

        style = Style(plotly_plot_mode=embed_chart)

        for c in self._util_func.dict_key_list(chart.keys()):
            html_output.append([self._chart.plot(chart[c], style=style)])

        return html_output

    @abc.abstractmethod
    def _layout_computation_results_to_html(self, embed_chart='offline_embed_js_div'):
        """Converts the computation results to a list containing HTML, primarily of the charts. Should be implemented
        by concrete subclasses, where we can select the order of the charts (and which charts are converted)

        Parameters
        ----------
        embed_chart : str
            'offline_embed_js_div' (default) - converts Plotly Figures into HTML + includes Plotly.js script
            'offline_div' - converts Plotly Figures into HTML (but excludes Plotly.js script)

        Returns
        -------
        list (containing HTML)
        """
        pass