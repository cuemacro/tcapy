from __future__ import print_function

__author__ = 'saeedamen'  # Saeed Amen / saeed@cuemacro.com

#
# Copyright 2018 Cuemacro Ltd. - http//www.cuemacro.com / @cuemacro
#
# See the License for the specific language governing permissions and limitations under the License.
#

import abc

from tcapy.conf.constants import Constants
from tcapy.util.loggermanager import LoggerManager
from tcapy.util.utilfunc import UtilFunc

ABC = abc.ABCMeta('ABC', (object,), {'__slots__': ()})

constants = Constants()

from datetime import datetime

class ComputationCaller(ABC):
    """Abstract class which adds listeners to the GUI buttons in the tcapy application for doing TCA or other _calculations. At
    initialisation it adds listeners for these buttons and links them to the various text box inputs (where the user
    can specify the various computation parameters such as start date, finish date, ticker, TCA metrics etc.)

    When a button is pressed it triggers various "calculate" methods, which convert the GUI input, into computation request/TCARequest objects
    which are then sent to another object for doing the actual computation. This analysis is then cached in Redis. The
    completion of this calculation will then trigger a callback from every display component (such as a plot or table)
    which search the cache for the appropriate output to display.

    If a user wishes to create programmatically call tcapy, it is recommended they create a comptuation request directly, rather
    than attempting to use ComputationCaller, and then submit that to an external computation engine.
    """

    def __init__(self, app, session_manager, callback_manager, glob_volatile_cache, layout, callback_dict=None):
        self._util_func = UtilFunc()

        self._session_manager = session_manager
        self._callback_manager = callback_manager

        self._glob_volatile_cache = glob_volatile_cache

        self.create_callbacks(app, callback_manager, callback_dict=callback_dict)

    def create_plot_flags(self, session_manager, layout):
        """Creates flags for each display component (eg. plot or table) on each web page in the project. These are
        necessary so we can keep track of whether we need to recalculate the underlying TCA analysis.

        Parameters
        ----------
        session_manager : SessionManager
            Stores and modifies session data which is unique for each user

        layout : Layout
            Specifies the layout of an HTML page using Dash components

        Returns
        -------
        dict
        """
        plot_flags = {};
        plot_lines = {}

        for page in layout.pages:

            page_flags = [];
            line_flags = []

            # For redrawing plots
            for gen_flag in self._generic_plot_flags:
                key = page + gen_flag

                # Append a plot flag if it exists
                if key in layout.id_flags:
                    page_flags.append(self._session_manager.create_calculated_flags(
                        'redraw-' + page, session_manager.create_calculated_flags(
                            self._util_func.dict_key_list(layout.id_flags[key].keys()),
                            self._generic_plot_flags[gen_flag])
                    ))

            plot_flags[page] = UtilFunc().flatten_list_of_lists(page_flags)

            # For clicking on charts
            for gen_flag in self._generic_line_flags:
                key = page + gen_flag

                # Append a line clicking flag if it exists
                if key in layout.id_flags:
                    line_flags.append(self._session_manager.create_calculated_flags(
                        'redraw-' + page,
                        session_manager.create_calculated_flags(
                            self._util_func.dict_key_list(layout.id_flags[key].keys()),
                            self._generic_plot_flags[gen_flag])
                    ))

            if line_flags != []:
                plot_lines[page] = UtilFunc().flatten_list_of_lists(line_flags)

        return plot_flags

    def create_callbacks(self, app, callback_manager, callback_dict=None):
        """Creates callbacks for each calculation button in the application, so that it is linked to execution code, when that
        button is pressed. Typically these button presses kick off a large computation (eg. TCA analysis).

        Parameters
        ----------
        app : dash.App
            A dash app is wrapper over a Flask mini-webserver

        callback_manager : CallbackManager
            Creates callbacks for dash components

        callback_dict : dict
            Dictionary of callbacks for Dash

        """

        if callback_dict is None:
            callback_dict = constants.dash_callbacks

        for k in callback_dict.keys():
            # Dash callbacks for detailed page
            app.callback(
                callback_manager.output_callback(k, 'status'),
                callback_manager.input_callback(k, callback_dict[k]))(
                self.calculate_computation_summary(k))

    def add_list_kwargs(self, kwargs, tag, addition):
        """Adds a value to the kwargs dictionary (or appends it to an existing _tag

        Parameters
        ----------
        kwargs : dict
            Existing kwargs dictionary

        tag : str
            Key to be added to kwargs

        addition : str
            Value of key to be added

        Returns
        -------
        dict
        """

        if addition is not None:
            if tag not in kwargs:
                kwargs[tag] = addition
            else:
                if kwargs[tag] is not None:
                    if isinstance(kwargs[tag], list):
                        kwargs[tag] = kwargs[tag].append(addition)
                    else:
                        kwargs[tag] = [kwargs[tag], addition]
                else:
                    kwargs[tag] = addition

        return kwargs

    def fill_computation_request_kwargs(self, kwargs, fields):
       pass

    def create_computation_request(self, **kwargs):
        pass

    def _fetch_cached_list(self, force_calculate=False, computation_type=None, session_id=None, key=None):
        """Fetches a cached list of objects (typically DataFrames) which have been generated during a larger computation
        (eg. TCA analysis) for a particular session.

        Parameters
        ----------
        force_calculate : bool (default: False)
            Should a large calculation be recomputed? If so, do not attempt to fetch from cache

        computation_type : str
            What computation type are we doing?

        session_id : str
            A unique identifer for the current web session

        key : str
            Which key to retrieve from the cache, which (usually) relates to a DataFrame generated by TCA output

        Returns
        -------
        list (usually of pd.DataFrames)
        """

        cached_list = []

        # First try to get from the cache (only need the key for this, no hash!)
        if not (force_calculate):
            if not (isinstance(key, list)):
                key = [key]

            if session_id != '' and computation_type != '':
                sessions_id_computation = session_id + '' + computation_type + '_'
            else:
                sessions_id_computation = ''

            for k in key:
                # this will be unique to each user
                cached_list.append(self._glob_volatile_cache.get(sessions_id_computation + k))

        return cached_list

    def get_cached_computation_analysis(self, **kwargs):
        """Fetches a computation outoput from a cache (typically Redis) or computes the analysis directly using another object, if
        requested. Typically, a computation is initiated and then that large analysis is cached, ready to be consumed by
        display components which repeatedly call this function.

        Parameters
        ----------
        kwargs
            Variables generated by GUI which relate to our computations (eg. start date, finish date, ticker etc.)

        Returns
        -------
        pd.DataFrame
        """

        try:
            force_calculate = kwargs['force_calculate']
        except:
            force_calculate = False

        key = None

        if 'key' in kwargs: key = kwargs['key']

        if 'test' not in kwargs:
            computation_type = self._tca_engine.get_engine_description()
            session_id = self._session_manager.get_session_id() + "_expiry_"
            session_id_computation = session_id + '' + computation_type + '_'
        else:
            computation_type = ''
            session_id = ''
            session_id_computation = ''

        # Try to fetch some TCA analysis output from the cache
        cached_list = self._fetch_cached_list(force_calculate=force_calculate, computation_type=computation_type,
                                              session_id=session_id, key=key)

        # Otherwise force the calculation (or if doesn't exist in the cache!)
        # when a button is pressed, typically force calculate will be set to True
        if force_calculate:

            computation_request = self.create_computation_request(**kwargs)

            # Delete any existing keys for the current session
            self._glob_volatile_cache.clear_key_match("*" + session_id + "*")

            dict_of_df = self.run_computation_request(computation_request)

            dict_key_list = [];
            dict_element_list = []

            # Cache all the dataframes in Redis/or other memory space (will likely need for later calls!)
            # from security perspective probably better not to cache the TCAEngine objects on a database (which can execute code)
            for dict_key in dict_of_df.keys():

                # check if we have all the keys filled (will be missing if for example there are no trades)
                if dict_key not in dict_of_df:
                    raise Exception('Missing ' + dict_key)

                dict_key_list.append(session_id_computation + dict_key);
                dict_element_list.append(dict_of_df[dict_key])

            self._session_manager.set_session_flag('user_df', dict_key_list)

            # self._glob_volatile_cache.put(session_id_computation + dict_key, dict_of_df[dict_key])

            # Put it back into Redis cache (to be fetched by Dash callbacks)
            self._glob_volatile_cache.put(dict_key_list, dict_element_list)

            logger = LoggerManager.getLogger(__name__)
            logger.debug('Generated tables: ' + str(self._util_func.dict_key_list(dict_of_df.keys())))

            if key is None:
                return None

            if not (isinstance(key, list)):
                key = [key]

            for k in key:
                # Has one of the dataframes we want, just been calculated, if so return it!
                if k in dict_of_df.keys():
                    cached_list.append(dict_of_df[k])

                # Otherwise look in Redis for the table for the user
                else:
                    # as last resort get from our global, this key is unique to each user
                    cached_list.append(self._glob_volatile_cache.get(session_id_computation + k))

        # return as tuples
        tup = list(cached_list)

        if len(tup) == 1:
            return tup[0]
        else:
            return tup

    def create_status_msg_flags(self, computation_type, ticker, calc_start, calc_end):
        if isinstance(ticker, list):
            ticker = self._util_func.pretty_str_list(ticker)

        title = ticker + ": " \
                + str(calc_start).replace(':00+00:00', '').replace('000+00:00', '') + " - " \
                + str(calc_end).replace(':00+00:00', '').replace('000+00:00', '') + " at " \
                + str(datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"))

        self._session_manager.set_session_flag(
            {computation_type + '-title': title, computation_type + '-ticker': ticker})

        self._session_manager.set_session_flag(self._plot_flags[computation_type], True)

        return title

    def create_generate_button_msg(self, old_clicks, n_clicks):
        return 'Triggered click old: ' + str(old_clicks) + " clicks vs new " + str(n_clicks) + \
               " for " + str(self._session_manager.get_session_id())

    def get_username_string(self):
        username = self._session_manager.get_username()

        if username is None:
            username = ''
        else:
            username = ' - ' + username

        return username

    @abc.abstractmethod
    def fill_computation_request_kwargs(self, kwargs, fields):
        """Fills a dictionary with the appropriate parameters which can be consumed by a ComputationRequest object. This involves
        a large number of object conversations, eg. str based dates to TimeStamps, metric names to Metric objects etc.

        Parameters
        ----------
        kwargs : dict
            Contains parameters related to computation analysis

        fields : str(list)
            List of fields we should fill with None if they don't exist in kwargs

        Returns
        -------
        dict
        """

        pass

    @abc.abstractmethod
    def run_computation_request(self, computation_request):
        """Creates a ComputationRequest object, populating its' fields with those from a kwargs dictionary, which consisted of
        parameters such as the start date, finish date, ticker, metrics to be computed, benchmark to be computed etd.

        The ComputationRequest object can later be consumed by a computation engine such as a TCAEngine

        Parameters
        ----------
        kwargs : dict
            For describing a computational analysis, such as the start date, finish date, ticker etc.

        Returns
        -------
        ComptuationRequest
        """
        pass

    @abc.abstractmethod
    def calculate_computation_summary(self, computation_type, external_params=None):
        """

        Parameters
        ----------
        comptuation_type : str
            Type of computation eg. 'detailed'

        external_params : dict


        Returns
        -------

        """
        pass