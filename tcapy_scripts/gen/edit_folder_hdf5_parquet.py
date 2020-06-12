"""Edits a folder of Parquet files to add a ticker column
"""

from __future__ import print_function, division

__author__ = 'saeedamen'  # Saeed Amen / saeed@cuemacro.com

#
# Copyright 2020 Cuemacro
#
# See the License for the specific language governing permissions and limitations under the License.
#

import os

from tcapy.util.loggermanager import LoggerManager
from tcapy.util.utilfunc import UtilFunc

add_vendor = 'dukascopy'

path = parquet_path = '/home/tcapyuser/csv_dump/' +  add_vendor + '/'

filenames = os.listdir(path)

util_func = UtilFunc()
logger = LoggerManager.getLogger(__name__)

for filename in filenames:
    format = filename.split('.')[-1]

    if format == 'gzip':
        format = 'parquet'
    elif format == 'h5':
        format = 'hdf5'

    logger.info('Reading to patch file ' + filename)

    df = util_func.read_dataframe_from_binary(os.path.join(path, filename), format=format)

    # Do your edits here, in this case overwriting the ticker column
    ticker = filename.split('_')[0]
    df['ticker'] = ticker

    util_func.write_dataframe_to_binary(df, os.path.join(path, filename), format=format)