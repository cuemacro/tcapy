from __future__ import print_function

__author__ = 'saeedamen'  # Saeed Amen / saeed@cuemacro.com

#
# Copyright 2018 Cuemacro Ltd. - http//www.cuemacro.com / @cuemacro
#
# See the License for the specific language governing permissions and limitations under the License.
#

import abc
import datetime
import base64
import os

import pdfkit

try:
    from weasyprint import HTML, CSS
except Exception as e:
    print("You may need to check that you've installed WeasyPrint - check instructions at https://github.com/cuemacro/tcapy/blob/master/INSTALL.md")
    print(str(e))

from jinja2 import Environment, FileSystemLoader

from chartpy import Canvas, Chart, Style

import plotly.graph_objs as go

from tcapy.conf.constants import Constants
from tcapy.util.utilfunc import UtilFunc

# compatible with Python 2 *and* 3:
ABC = abc.ABCMeta('ABC', (object,), {'__slots__': ()})

constants = Constants()

class Renderer(object):
    """Creates HTML pages based on a dictionary input full of charts and can also convert them to PDF. Can both write
    HTML/PDF to disk or return as a string, which is easier to consume by certain callers such as a webserver.

    """

    def __init__(self):
        self._util_func = UtilFunc()

    @abc.abstractmethod
    def render_elements(self, elements_to_render_dict, title=None, output_filename=None, output_format='html', extra_head_code=None):
        """Takes a dictionary of elements, typically a mixture of charts (usually in plotly.Fig format) and text which
        are to be rendered into HTML (and subsequentally PDF)

        Parameters
        ----------
        elements_to_render_dict : dict
            Objects to render as HTML

        title : str
            Title of page to be rendered

        output_filename : str
            Path of the HTML/PDF to be dumped on disk

        output_format : str
            'html' - create HTML output
            'pdf' - create PDF output

        extra_head_code : str
            HTML code to put into the <head> _tag

        Returns
        -------
        str (HTML/PDF)
        """
        pass

    def write_presentation_output(self, html, output_filename=None, output_format='html', pdf_converter='weasyprint',
                                  stylesheets=None):
        """Writes HTML/PDF to disk and also returns the HTML/PDF as a string so it can be consumed elsewhere

        Parameters
        ----------
        html : str
            HTML string to be written/returned

        output_filename : str
            Output filename (if this is None, will return str of either HTML or PDF)

        output_format : str
            'html' - create HTML output
            'pdf' - create PDF output

        pdf_converter : str
            'weasyprint' - default
            'pdfkit' - (optional)
        stylesheets : str
            CSS format

        Returns
        -------
        str (HTML or PDF)
        """

        out = None

        if output_format == 'html':

            if output_filename is not None:
                html_file = open(output_filename, "w", encoding="utf-8")
                html_file.write(html)
                html_file.close()

            out = html

        elif output_format == 'pdf':
            if pdf_converter == 'pdfkit':
                if output_filename is None:
                    out = pdfkit.from_string(html, False)
                else:
                    out = pdfkit.from_string(html, output_filename)
            elif pdf_converter == 'weasyprint':
                out = HTML(string=html).write_pdf(output_filename, stylesheets=stylesheets)

        return out


class CanvasRenderer(Renderer):
    """Uses the Canvas object from chartpy to create HTMLs and renders them to PDF using pdfkit/weasyprint

    """

    def __init__(self, canvas_plotter='plain'):
        super(CanvasRenderer, self).__init__()
        self._canvas_plotter = canvas_plotter

    def render_elements(self, elements_to_render_dict, title=None, output_filename=None, output_format='html',
                        pdf_converter='pdfkit', extra_head_code=None):

        # Only take charts and tables, and ignore text
        if 'charts' in elements_to_render_dict.keys() and 'tables' in elements_to_render_dict.keys():
            elements_to_render_dict = {**elements_to_render_dict['charts'], **elements_to_render_dict['tables']}
        elif 'charts' in elements_to_render_dict.keys():
            elements_to_render_dict = elements_to_render_dict['charts']
        elif 'tables' in elements_to_render_dict.keys():
            elements_to_render_dict = elements_to_render_dict['tables']

        elements_to_render = self._util_func.flatten_list_of_lists(list(elements_to_render_dict.values()))

        canvas = Canvas(elements_to_render=elements_to_render)

        # Generate HTML string with chartpy's Canvas object
        html, _ = canvas.generate_canvas(output_filename=output_filename, silent_display=True,
                                         canvas_plotter=self._canvas_plotter, page_title=title,
                                         render_pdf=False,
                                         return_html_binary=True, extra_head_code=extra_head_code)

        if output_format not in ['html', 'pdf']:
            raise Exception("Invalid output format selected")

        # Taking HTML as input write to disk as HTML file (or PDF), and return the binary representation (which can
        # be more easily handled by eg. a web server)
        return self.write_presentation_output(html, output_filename=output_filename, output_format=output_format,
                                              pdf_converter=pdf_converter)

class JinjaRenderer(Renderer):
    """Uses Jinja HTML templates to create HTML reports and WeasyPrint to convert them into PDFs

    Reference: https://pbpython.com/pdf-reports.html
    """

    def __init__(self, html_template='clean_report.html', logo_file='cuemacro_logo.png',
                 template_folder=constants.root_folder + "/vis/report/templates/"):

        super(JinjaRenderer, self).__init__()

        self._html_template = html_template
        self._logo_path = template_folder + logo_file
        self._template_folder = template_folder

    def render_elements(self, elements_to_render_dict, title=None, output_filename=None, output_format='html',
                        pdf_converter='weasyprint', offline_js=False,
                        extra_head_code=None):

        env = Environment(loader=FileSystemLoader(searchpath=self._template_folder))

        template = env.get_template(self._html_template)

        if output_format not in ['html', 'pdf']:
            raise Exception("Invalid output format selected")

        for k in elements_to_render_dict.keys():
            for m in elements_to_render_dict[k].keys():
                for n in range(0, len(elements_to_render_dict[k][m])):
                    if isinstance(elements_to_render_dict[k][m][n], list):
                        elements_to_render_dict[k][m][n] = elements_to_render_dict[k][m][n][0]

        logo = None

        if os.path.exists(self._logo_path):
            with open(self._logo_path, 'rb') as img_f:
                logo = base64.b64encode(img_f.read()).decode('utf8')

        # Add timestamp for generation date/time
        generation_date = str(datetime.datetime.utcnow().strftime("%b %d %Y %H:%M"))

        html = template.render(elements_to_render_dict=elements_to_render_dict, logo=logo, generation_date=generation_date)

        # Taking HTML as input write to disk as HTML file (or PDF), and return the binary representation (which can
        # be more easily handled by eg. a web server)
        return self.write_presentation_output(html, output_filename=output_filename, output_format=output_format,
                                              pdf_converter=pdf_converter)

class XlWingsRenderer(Renderer):
    """Uses xlwings to add the report to a live Excel sheet

    """

    def __init__(self, xlwings_sht=None):
        super(XlWingsRenderer, self).__init__()
        self._xlwings_sht = xlwings_sht

    def render_elements(self, elements_to_render_dict, title=None, output_filename=None, output_format='xlwings',
                        extra_head_code=None):

        if output_format not in ['xlwings']:
            raise Exception("Invalid output format selected")

        # Clear the TCA properties
        self._xlwings_sht.range('listpoints').clear_contents()

        # Delete all the TCA pictures/charts except for the logo (first picture!)
        # no_of_pictures = len(self._xlwings_sht.pictures)

        for pic in self._xlwings_sht.pictures:
            if 'logo' not in pic.name:
                pic.delete()


        # Start putting the TCA properties in this row onwards
        row = constants.chart_xlwings_listpoints_row

        # listpoints
        for pts in elements_to_render_dict['listpoints'].keys():
            val = elements_to_render_dict['listpoints'][pts]

            self._xlwings_sht.range('B' + str(row)).value = pts
            self._xlwings_sht.range('C' + str(row)).value = val

            row = row + 1

        # create a gap to the next charts
        row = (constants.chart_xlwings_top_row) * constants.chart_xlwings_row_height
        top = 12 * constants.chart_xlwings_row_height + row
        left = 50

        timestamp = str(datetime.datetime.now()).replace(':', '-').replace(' ', '-').replace(".", "-")
        filename = "fig_tcapy" + timestamp + ".png"

        # charts
        for c in elements_to_render_dict['charts'].keys():

            # Eg. 'Heatmap charts'
            mini_charts = elements_to_render_dict['charts'][c]

            for m in mini_charts:
                for n in m:
                    if isinstance(n, go.Figure):
                        n.write_image(filename)
                        self._xlwings_sht.pictures.add(filename, top=top, left=left,
                                                       width=constants.chart_xlwings_report_width,
                                                       height=constants.chart_xlwings_report_height)

                        top = top + constants.chart_xlwings_report_height \
                              + constants.chart_xlwings_vertical_gap

        try:
            os.remove(filename)
        except:
            pass

        # tables
        # TODO

class ComputationReport(ABC):
    """Converts ComputationResults (largely consisting of Plotly based Figures and HTML tables) into self contained HTML pages.
    Can also render these HTML pages into PDFs. Uses Renderer objects to create the HTML including BasicRenderer (which
    uses chartpy's "Canvas" object extensively) and JinjaRenderer (uses Jinja templating for HTML and WeasyPrint for PDF
    conversion).

    """

    def __init__(self, computation_results, title='Cuemacro Computation', renderer=CanvasRenderer(),
                 chart_report_height=constants.chart_report_height, chart_report_width=constants.chart_report_width):
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
        self._chart = Chart(engine='plotly')
        self._renderer = renderer
        self._computation_request = computation_results.computation_request

        self._chart_report_width = chart_report_width
        self._chart_report_height = chart_report_height

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
            True - includes Plotly.js in web page to be rendered (results in much bigger file sizes)

        Returns
        -------
        pdf or HTML binary

        """

        extra_head_code = ''

        if output_format == 'html':

            # Embed plotly.js in HTML (makes it bigger, but then doesn't require web connection)
            if offline_js:
                embed_chart = 'offline_embed_js_div'
            else:
                # Otherwise put web link to plotly.js (but this means we need to download every time)
                embed_chart = 'offline_div'
                extra_head_code = '<head><script src="https://cdn.plot.ly/plotly-latest.min.js"></script></head>'
        elif output_format == 'pdf':
            # For PDFs we need to create static SVGs of plotly charts
            embed_chart = 'offline_image_svg_in_html'
        elif output_format == 'xlwings':
            embed_chart = 'leave_as_fig'

        # Get a list of the HTML to render
        elements_to_render_dict = self._layout_computation_results_to_html(embed_chart)

        return self._renderer.render_elements(elements_to_render_dict, title=self._title, output_filename=output_filename,
                                            output_format=output_format, extra_head_code=extra_head_code)

    def _generate_filename(self, extension):
        return (self._get_time_stamp() + "." + extension)

    def _get_time_stamp(self):
        return str(datetime.datetime.now()).replace(':', '-').replace(' ', '-').replace(".", "-")

    def _create_text_html(self, text, add_hr=True):
        """Takes text and then creates the appropriate HTML to represent it, split by horizontal HTML bars

        Parameters
        ----------
        text : str (list)
            Text to be added in HTML

        Returns
        -------
        list (of HTML)
        """
        if text != [] and text is not None and add_hr:
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

            # Update chart size and padding (if it's Plotly), so it fits well on PDF
            try:
                chart[c].update_layout(
                    autosize=False,
                    width=self._chart_report_width,
                    height=self._chart_report_height,
                    margin=dict(
                        l=10,
                        r=10,
                        b=10,
                        t=60,
                        pad=4
                    ),
                )
            except:
                pass

            if embed_chart == 'leave_as_fig':
                html_output.append([chart[c]])
            else:
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
        list (containing HTML), list (containing HTML of descriptions)
        """
        pass


