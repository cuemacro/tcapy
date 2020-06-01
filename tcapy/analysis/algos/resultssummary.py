from __future__ import print_function, division

__author__ = 'saeedamen'  # Saeed Amen / saeed@cuemacro.com

#
# Copyright 2017 Cuemacro Ltd. - http//www.cuemacro.com / @cuemacro
#
# See the License for the specific language governing permissions and limitations under the License.
#

import numpy as np
import pandas as pd

from scipy.stats.kde import gaussian_kde
from scipy.stats import norm
from statsmodels.stats.weightstats import DescrStatsW

from tcapy.util.mediator import Mediator

class ResultsSummary(object):
    """Calculates summarised statistics for a group of trades. This includes calculating average slippage for each
    group of trades and also creating histograms of these observations (and fitting it alongside a PDF/probability
    distribution function).

    """

    def __init__(self):
        self._time_series_ops = Mediator.get_time_series_ops()
        self._util_func = Mediator.get_util_func()

    def _create_histogram_distribution(self, df, min_x=None, max_x=None, extend_x_proportion_percentage=20,
                                       postfix_label=None, obs_weights=None, denormalised=True):

        # get min/max values for our histogram
        min_hist_x = df.min()
        max_hist_x = df.max()

        extend_x_proportion_percentage = 1.0 + (float(extend_x_proportion_percentage) / 100.0)

        # extend axes for PDF, so just outside histogram
        if min_x is not None:
            min_x = min(min_x, min_hist_x) * extend_x_proportion_percentage
        else:
            min_x = min_hist_x

        if max_x is not None:
            max_x = max(max_x, max_hist_x) * extend_x_proportion_percentage
        else:
            max_x = max_hist_x

        if denormalised: density = False

        vals = df.T.values.astype(np.float64)

        # Create a histogram with 10 buckets
        hist, bins = np.histogram(vals, bins=10, range=[float(min_hist_x), float(max_hist_x)], density=density, weights=obs_weights)
        bin_cent = (bins[1:] + bins[:-1]) * 0.5

        number_of_elements = len(df.values)

        dist_space = np.linspace(min_x, max_x, 100)

        if postfix_label is None:
            postfix_label = ''
        else:
            postfix_label = ": " + postfix_label

        if number_of_elements > 1:

            # Create a best fit PDF using Gaussian KDE model (forcibly cast to float64)
            if obs_weights is None:
                kde = gaussian_kde(vals)
            else:
                kde = gaussian_weighted_kde(vals, weights=obs_weights.values.astype(np.float64))

            # Sometimes need to transpose so the dimensions are consistent
            try:
                pdf_fit = kde(dist_space)
            except:
                pdf_fit = kde(dist_space.T)

            if obs_weights is None:
                # Calculated normal PDF
                weighted_stats = DescrStatsW(df.values, ddof=0)
            else:
                weighted_stats = DescrStatsW(df.values, weights=obs_weights.T.values, ddof=0)

            mu = weighted_stats.mean
            std = weighted_stats.std

            normal_pdf_fit = norm.pdf(dist_space, mu, std)

            # Scale pdf_fit (and normal PDF) by total/bin size
            if denormalised:
                bin_width = abs(bins[1] - bins[0])
                N = np.sum(hist)
                pdf_fit = pdf_fit * (bin_width * N)
                normal_pdf_fit = normal_pdf_fit * (bin_width * N)

            df_hist = pd.DataFrame(index=bin_cent, data=hist, columns=['Histogram' + postfix_label])
            df_pdf = pd.DataFrame(index=dist_space, data=pdf_fit, columns=['KDE-PDF' + postfix_label])
            df_pdf['Norm-PDF' + postfix_label] = normal_pdf_fit
        else:
            return pd.DataFrame(), pd.DataFrame()

        return df_hist, df_pdf

    def field_distribution(self, metric_df, market_df=None, bid_benchmark='bid', mid_benchmark='mid',
                           ask_benchmark='ask', bid_avg=None, ask_avg=None,
                           aggregate_by_field=None, pdf_only=False, postfix_label=None, metric_name='slippage',
                           weighting_field=None, scalar=10000.0):
        """Fits a PDF across the slippage across a group of trades and also calculates a histogram, through bucketing
        the slippage of a group of trades.

        Parameters
        ----------
        metric_df : DataFrame
            Contains trade data with the calculated slippage metrics

        market_df : DataFrame (optional), default None
            Contains market data, which is required if we want to calculate average bid-to-mid and ask-to-mid spreads

        bid_benchmark : str (optional), default 'bid'
            Field to use for bid data

        mid_benchmark : str (optional), default 'mid'
            Field to use for mid data

        ask_benchmark : str (optional), default 'ask'
            Field to use for ask data

        bid_avg : float (optional), default None
            Average spread for bid to mid (in bp)

        ask_avg : float (optional), default None
            Average spread for ask to mid (in bp)

        aggregate_by_field : str (optional), default None
            Aggregate slippage by particular fields, such as the 'venue'

        pdf_only : bool (optional), default False
            Should we only display the fitting PDF (and not the histogram)

        postfix_label : str (optional), default None
            Label to be added to the end of each column of the DataFrame

        metric_name : str, default slippage
            The field to use for defining slippage

        weighting_field : str
            Should observations by weighted by a particular field (eg. notional)

        scalar : float (default: 10000.0)
            Should we multiply all numbers by scalar (typically to convert into basis point)

        Returns
        -------
        DataFrame
        """

        # calculate the average bid/ask values from market data if they haven't already been specified
        # convert to basis points
        if bid_avg is None and ask_avg is None and market_df is not None:
            # will fail for time series which only contain mid values
            try:
                bid_avg = ((market_df[bid_benchmark] / market_df[mid_benchmark]) - 1.0).mean() * float(scalar)
                ask_avg = ((market_df[ask_benchmark] / market_df[mid_benchmark]) - 1.0).mean() * float(scalar)
            except:
                pass

        # Ff the metric field doesn't exist, we cannot calculate anything!
        if metric_name not in metric_df.columns:
            raise Exception(metric_name + " field not found cannot calculate distribution!")

        obs_weights = None

        if weighting_field is not None and weighting_field in metric_df.columns:
            obs_weights = metric_df[weighting_field]

            # Check that obs_weights don't add up to zero... in which case just use equal weighting
            obs_weights_total = obs_weights.abs().sum()

            if obs_weights_total == np.nan:
                obs_weights = None
            elif obs_weights_total == 0:
                obs_weights = None

        # if postfix_label is None: postfix_label = ''

        # If we don't want to aggregate by any specific field
        if aggregate_by_field is None:

            # Convert slippage into basis points
            metric_sub_df = metric_df[metric_name] * float(scalar)

            df_hist, df_pdf = self._create_histogram_distribution(metric_sub_df, min_x=bid_avg, max_x=ask_avg,
                                                                  postfix_label=postfix_label, obs_weights=obs_weights)

            if pdf_only:
                df = df_pdf
            else:
                df = df_pdf.join(df_hist, how='outer')
        else:
            # Do we want it to aggregate results by a specific field? (eg. get distribution by the venue?)
            df_list = []

            for field_val, df_g in metric_df.groupby([aggregate_by_field]):
                metric_sub_df = df_g[metric_name] * 10000.0

                obs_weights = None

                if weighting_field is not None:
                    obs_weights = df_g[weighting_field]

                if postfix_label is None:
                    lab = str(field_val)
                else:
                    lab = postfix_label + ' ' + str(field_val)

                df_hist, df_pdf = self._create_histogram_distribution(metric_sub_df, min_x=bid_avg, max_x=ask_avg,
                                                                      postfix_label=lab,
                                                                      obs_weights=obs_weights)

                if not(df_hist.empty) and not(df_pdf.empty):
                    if not (pdf_only):
                        df_list.append(df_hist);

                    df_list.append(df_pdf)

            if df_list == []:
                df = pd.DataFrame()
            else:
                df = self._time_series_ops.outer_join(df_list)

        # TODO add bid/ask columns and mid

        if market_df is not None:
            if bid_avg is not None:
                df['Bid'] = bid_avg
            if ask_avg is not None:
                df['Ask'] = ask_avg

        return df

    def field_bucketing(self, trade_df, metric_name='slippage', aggregation_metric='mean',
                        aggregate_by_field='venue', by_date=None, weighting_field=None):
        """Calculates the "average" for a particular field and aggregates it by venue/asset etc. The average can be specified
        as the mean, or other metrics such as totals, number of trades etc.

        Parameters
        ----------
        trade_df : DataFrame
            Contains trade data by the client contains fields such as trade time, notional, side, price, etc.

        calculation_field : str, default 'signed_slippage'
            Which field to run statistics on such as the absolute slippage or signed slippage

        aggregation_metric : str {'mean', 'sum', 'count'}, default mean
            How should the data be aggregated

        aggregate_by_field : str, default 'venue'
            How should we aggregate our calculations (eg. by 'venue')

        by_date : str
            Should we aggregate our results by date, to create a timeline
            'date' - aggregate by date
            'datehour' - aggregate by date/hour
            'month' - aggregate by month
            'hour' - aggregate by hour
            'time' - aggregate by time

        Returns
        -------
        DataFrame
        """

        # TODO weighting field

        # eg. aggregate output by 'vendor' and calculate the average slippage per 'vendor'

        group = [aggregate_by_field]

        if by_date is not None:
            if by_date == 'date':
                group = [trade_df.index.date]
            elif by_date == 'datehour':
                trade_df.index = trade_df.index.floor('H')
                group = [trade_df.index]

            elif by_date == 'month':
                group = [trade_df.index.day]

            elif by_date == 'day':
                group = [trade_df.index.day]

            elif by_date == 'hour':
                group = [trade_df.index.hour]

            elif by_date == 'time':
                group = [trade_df.index.time]

            elif by_date == 'timeldn':
                group = [trade_df.index.copy().tz_convert('Europe/London').time]

            elif by_date == 'timenyc':
                group = [trade_df.index.copy().tz_convert('America/New_York').time]

            elif by_date == 'hourldn':
                group = [trade_df.index.copy().tz_convert('Europe/London').hour]

            elif by_date == 'hournyc':
                group = [trade_df.index.copy().tz_convert('America/New_York').hour]

            if aggregate_by_field is not None:
                group.append(aggregate_by_field)

        displayed_fields = [metric_name, weighting_field]

        if weighting_field is None:
            displayed_fields = [metric_name]

        # remove duplicated list elements
        displayed_fields = self._util_func.remove_duplicated_str(displayed_fields)

        group = [x for x in group if x is not None]

        agg = trade_df.groupby(group)[displayed_fields]

        # def weighted_avg(group, avg_name, weight_name):
        #     """ http://stackoverflow.com/questions/10951341/pd-dataframe-aggregate-function-using-multiple-columns
        #     In rare instance, we may not have weights, so just return the mean. Customize this if your business case
        #     should return otherwise.
        #     """
        #
        #     d = group[avg_name]
        #     w = group[weight_name]
        #     try:
        #         return (d * w).sum() / w.sum()
        #     except ZeroDivisionError:
        #         return d.mean()

        if aggregation_metric == 'mean':
            if weighting_field is None:
                agg = agg.mean()

            else:
                # Calculate a weighted average of the metric for each group
                agg = agg.apply(self._time_series_ops.weighted_average_lambda, metric_name, weighting_field)

        elif aggregation_metric == 'sum':
            agg = agg.sum()

        elif aggregation_metric == 'count':
            agg = agg.count()

        elif aggregation_metric == 'max':
            agg = agg.max()

        elif aggregation_metric == 'min':
            agg = agg.min()


        else:
            return Exception(aggregation_metric + " is not a valid aggregation, must be one of mean, sum or count.")

        df = pd.DataFrame(agg).transpose()

        if (by_date is not None):
            df = df.melt()
            df = df.set_index(df[df.columns[0]])
            df.index.name = 'Date'
            df = df.drop([df.columns[0]], axis=1)

            df = pd.pivot_table(df, index='Date', columns=aggregate_by_field, values=df.columns[-1])
        else:
            df = pd.pivot_table(df, index=aggregate_by_field, values=df.columns).transpose()

        return pd.DataFrame(df)

    def query_trade_order_population(self, trade_df, query_fields=['ticker', 'broker_id']):
        """Finds the unique values for particular fields, such as 'ticker'. Can be useful for working out which assets
        to add to our available universe list (same for brokers etc.)

        Parameters
        ----------
        trade_df : DataFrame
            Trade/orders

        query_fields : str (list)
            Fields to search for

        Returns
        -------
        dict
        """

        if not(isinstance(query_fields, list)): query_fields = [query_fields]

        query_dict = {}

        for q in query_fields:
            if q in trade_df.columns:
                x = trade_df[q].unique().tolist()
                x.sort()

                query_dict[q] = x

        return query_dict


####### Rewritten version of scipy's Gaussian KDE below allows weighting of points #####################################

# Original source http://nbviewer.jupyter.org/gist/tillahoffmann/f844bce2ec264c1c8cb5

from scipy.spatial.distance import cdist

import six

class gaussian_weighted_kde(object):
    """Representation of a kernel-density estimate using Gaussian kernels.

    Kernel density estimation is a way to estimate the probability density
    function (PDF) of a random variable in a non-parametric way.
    `gaussian_kde` works for both uni-variate and multi-variate data.   It
    includes automatic bandwidth determination.  The estimation works best for
    a unimodal distribution; bimodal or multi-modal distributions tend to be
    oversmoothed.

    Parameters
    ----------
    dataset : array_like
        Datapoints to estimate from. In case of univariate data this is a 1-D
        array, otherwise a 2-D array with shape (# of dims, # of data).
    bw_method : str, scalar or callable, optional
        The method used to calculate the estimator bandwidth.  This can be
        'scott', 'silverman', a scalar constant or a callable.  If a scalar,
        this will be used directly as `kde.factor`.  If a callable, it should
        take a `gaussian_kde` instance as only parameter and return a scalar.
        If None (default), 'scott' is used.  See Notes for more details.
    weights : array_like, shape (n, ), optional, default: None
        An array of weights, of the same shape as `x`.  Each value in `x`
        only contributes its associated weight towards the bin count
        (instead of 1).

    Attributes
    ----------
    dataset : ndarray
        The dataset with which `gaussian_kde` was initialized.
    d : int
        Number of dimensions.
    n : int
        Number of datapoints.
    neff : float
        Effective sample size using Kish's approximation.
    factor : float
        The bandwidth factor, obtained from `kde.covariance_factor`, with which
        the covariance matrix is multiplied.
    covariance : ndarray
        The covariance matrix of `dataset`, scaled by the calculated bandwidth
        (`kde.factor`).
    inv_cov : ndarray
        The inverse of `covariance`.

    Methods
    -------
    kde.evaluate(points) : ndarray
        Evaluate the estimated pdf on a provided set of points.
    kde(points) : ndarray
        Same as kde.evaluate(points)
    kde.pdf(points) : ndarray
        Alias for ``kde.evaluate(points)``.
    kde.set_bandwidth(bw_method='scott') : None
        Computes the bandwidth, i.e. the coefficient that multiplies the data
        covariance matrix to obtain the kernel covariance matrix.
        .. versionadded:: 0.11.0
    kde.covariance_factor : float
        Computes the coefficient (`kde.factor`) that multiplies the data
        covariance matrix to obtain the kernel covariance matrix.
        The default is `scotts_factor`.  A subclass can overwrite this method
        to provide a different method, or set it through a call to
        `kde.set_bandwidth`.

    Notes
    -----
    Bandwidth selection strongly influences the estimate obtained from the KDE
    (much more so than the actual shape of the kernel).  Bandwidth selection
    can be done by a "rule of thumb", by cross-validation, by "plug-in
    methods" or by other means; see [3]_, [4]_ for reviews.  `gaussian_kde`
    uses a rule of thumb, the default is Scott's Rule.

    Scott's Rule [1]_, implemented as `scotts_factor`, is::

        n**(-1./(d+4)),

    with ``n`` the number of data points and ``d`` the number of dimensions.
    Silverman's Rule [2]_, implemented as `silverman_factor`, is::

        (n * (d + 2) / 4.)**(-1. / (d + 4)).

    Good general descriptions of kernel density estimation can be found in [1]_
    and [2]_, the mathematics for this multi-dimensional implementation can be
    found in [1]_.

    References
    ----------
    .. [1] D.W. Scott, "Multivariate Density Estimation: Theory, Practice, and
           Visualization", John Wiley & Sons, New York, Chicester, 1992.
    .. [2] B.W. Silverman, "Density Estimation for Statistics and Data
           Analysis", Vol. 26, Monographs on Statistics and Applied Probability,
           Chapman and Hall, London, 1986.
    .. [3] B.A. Turlach, "Bandwidth Selection in Kernel Density Estimation: A
           Review", CORE and Institut de Statistique, Vol. 19, pp. 1-33, 1993.
    .. [4] D.M. Bashtannyk and R.J. Hyndman, "Bandwidth selection for kernel
           conditional density estimation", Computational Statistics & Data
           Analysis, Vol. 36, pp. 279-298, 2001.

    Examples
    --------
    Generate some random two-dimensional data:

    >>> from scipy import stats
    >>> def measure(n):
    >>>     "Measurement model, return two coupled measurements."
    >>>     m1 = np.random.normal(size=n)
    >>>     m2 = np.random.normal(scale=0.5, size=n)
    >>>     return m1+m2, m1-m2

    >>> m1, m2 = measure(2000)
    >>> xmin = m1.min()
    >>> xmax = m1.max()
    >>> ymin = m2.min()
    >>> ymax = m2.max()

    Perform a kernel density estimate on the data:

    >>> X, Y = np.mgrid[xmin:xmax:100j, ymin:ymax:100j]
    >>> positions = np.vstack([X.ravel(), Y.ravel()])
    >>> values = np.vstack([m1, m2])
    >>> kernel = stats.gaussian_kde(values)
    >>> Z = np.reshape(kernel(positions).T, X.shape)

    Plot the results:

    >>> import matplotlib.pyplot as plt
    >>> fig = plt.figure()
    >>> ax = fig.add_subplot(111)
    >>> ax.imshow(np.rot90(Z), cmap=plt.cm.gist_earth_r,
    ...           extent=[xmin, xmax, ymin, ymax])
    >>> ax.plot(m1, m2, 'k.', markersize=2)
    >>> ax.set_xlim([xmin, xmax])
    >>> ax.set_ylim([ymin, ymax])
    >>> plt.show()

    """

    def __init__(self, dataset, bw_method=None, weights=None):
        self.dataset = np.atleast_2d(dataset)
        if not self.dataset.size > 1:
            raise ValueError("`dataset` input should have multiple elements.")
        self.d, self.n = self.dataset.shape

        if weights is not None:
            self.weights = weights / np.sum(weights)
        else:
            self.weights = np.ones(self.n) / self.n

        # Compute the effective sample size
        # http://surveyanalysis.org/wiki/Design_Effects_and_Effective_Sample_Size#Kish.27s_approximate_formula_for_computing_effective_sample_size
        self.neff = 1.0 / np.sum(self.weights ** 2)

        self.set_bandwidth(bw_method=bw_method)

    def evaluate(self, points):
        """Evaluate the estimated pdf on a set of points.

        Parameters
        ----------
        points : (# of dimensions, # of points)-array
            Alternatively, a (# of dimensions,) vector can be passed in and
            treated as a single point.

        Returns
        -------
        values : (# of points,)-array
            The values at each point.

        Raises
        ------
        ValueError : if the dimensionality of the input points is different than
                     the dimensionality of the KDE.

        """
        points = np.atleast_2d(points)

        d, m = points.shape
        if d != self.d:
            if d == 1 and m == self.d:
                # points was passed in as a row vector
                points = np.reshape(points, (self.d, 1))
                m = 1
            else:
                msg = "points have dimension %s, dataset has dimension %s" % (d,
                                                                              self.d)
                raise ValueError(msg)

        # compute the normalised residuals
        chi2 = cdist(points.T, self.dataset.T, 'mahalanobis', VI=self.inv_cov) ** 2
        # compute the pdf
        result = np.sum(np.exp(-.5 * chi2) * self.weights, axis=1) / self._norm_factor

        return result

    __call__ = evaluate

    def scotts_factor(self):
        return np.power(self.neff, -1. / (self.d + 4))

    def silverman_factor(self):
        return np.power(self.neff * (self.d + 2.0) / 4.0, -1. / (self.d + 4))

    #  Default method to calculate bandwidth, can be overwritten by subclass
    covariance_factor = scotts_factor

    def set_bandwidth(self, bw_method=None):
        """Compute the estimator bandwidth with given method.

        The new bandwidth calculated after a call to `set_bandwidth` is used
        for subsequent evaluations of the estimated density.

        Parameters
        ----------
        bw_method : str, scalar or callable, optional
            The method used to calculate the estimator bandwidth.  This can be
            'scott', 'silverman', a scalar constant or a callable.  If a
            scalar, this will be used directly as `kde.factor`.  If a callable,
            it should take a `gaussian_kde` instance as only parameter and
            return a scalar.  If None (default), nothing happens; the current
            `kde.covariance_factor` method is kept.

        Notes
        -----
        .. versionadded:: 0.11

        Examples
        --------
        >>> x1 = np.array([-7, -5, 1, 4, 5.])
        >>> kde = stats.gaussian_kde(x1)
        >>> xs = np.linspace(-10, 10, num=50)
        >>> y1 = kde(xs)
        >>> kde.set_bandwidth(bw_method='silverman')
        >>> y2 = kde(xs)
        >>> kde.set_bandwidth(bw_method=kde.factor / 3.)
        >>> y3 = kde(xs)

        >>> fig = plt.figure()
        >>> ax = fig.add_subplot(111)
        >>> ax.plot(x1, np.ones(x1.shape) / (4. * x1.size), 'bo',
        ...         label='Data points (rescaled)')
        >>> ax.plot(xs, y1, label='Scott (default)')
        >>> ax.plot(xs, y2, label='Silverman')
        >>> ax.plot(xs, y3, label='Const (1/3 * Silverman)')
        >>> ax.legend()
        >>> plt.show()

        """
        if bw_method is None:
            pass
        elif bw_method == 'scott':
            self.covariance_factor = self.scotts_factor
        elif bw_method == 'silverman':
            self.covariance_factor = self.silverman_factor
        elif np.isscalar(bw_method) and not isinstance(bw_method, six.string_types):
            self._bw_method = 'use constant'
            self.covariance_factor = lambda: bw_method
        elif callable(bw_method):
            self._bw_method = bw_method
            self.covariance_factor = lambda: self._bw_method(self)
        else:
            msg = "`bw_method` should be 'scott', 'silverman', a scalar " \
                  "or a callable."
            raise ValueError(msg)

        self._compute_covariance()

    def _compute_covariance(self):
        """Computes the covariance matrix for each Gaussian kernel using
        covariance_factor().
        """
        self.factor = self.covariance_factor()
        # Cache covariance and inverse covariance of the data
        if not hasattr(self, '_data_inv_cov'):
            # Compute the mean and residuals
            _mean = np.sum(self.weights * self.dataset, axis=1)
            _residual = (self.dataset - _mean[:, None])
            # Compute the biased covariance
            self._data_covariance = np.atleast_2d(np.dot(_residual * self.weights, _residual.T))
            # Correct for bias (http://en.wikipedia.org/wiki/Weighted_arithmetic_mean#Weighted_sample_covariance)
            self._data_covariance /= (1 - np.sum(self.weights ** 2))
            self._data_inv_cov = np.linalg.inv(self._data_covariance)

        self.covariance = self._data_covariance * self.factor ** 2
        self.inv_cov = self._data_inv_cov / self.factor ** 2
        self._norm_factor = np.sqrt(np.linalg.det(2 * np.pi * self.covariance))  # * self.n
