"""Runs an aggregated TCA calculation for all currency pairs for the past _year. This will cache all the market and
trade data on Redis. Hence subsequent calls should mostly be using Redis, when downloading full month (day/week) data
(and calling the underlying trade/market databases only for smaller portions of data < 1 day, 1 week etc).
"""

from __future__ import print_function, division

__author__ = 'saeedamen'  # Saeed Amen / saeed@cuemacro.com

#
# Copyright 2017 Cuemacro Ltd. - http//www.cuemacro.com / @cuemacro
#
# See the License for the specific language governing permissions and limitations under the License.
#

if __name__ == '__main__':

    # Need this for WINDOWS machines, to ensure multiprocessing stuff works properly
    from tcapy.util.swim import Swim;

    Swim()

    from tcapy.data.volatilecache import VolatileRedis

    import datetime;
    from datetime import timedelta

    # First delete the Redis cache
    volatile = VolatileRedis()
    volatile.clear_cache()

    from tcapy.analysis.tcaengine import TCARequest, TCAEngineImpl

    tca_engine = TCAEngineImpl()

    # Do a massive TCA computation for all currency pairs for the past _year
    # this will cache all the data in Redis, which can be used later
    finish_date = datetime.datetime.utcnow().date() - timedelta(days=1)
    start_date = finish_date - timedelta(days=252)

    tca_request = TCARequest(start_date=start_date, finish_date=finish_date, ticker='All')
    tca_engine.calculate_tca(tca_request)
