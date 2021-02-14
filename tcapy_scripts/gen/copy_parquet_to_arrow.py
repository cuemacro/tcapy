"""Copies a folder of parquet files into another into arrow folder for use with vaex. Note you need to install vaex
library in addition to use this.
"""

from __future__ import print_function, division

__author__ = 'saeedamen'  # Saeed Amen / saeed@cuemacro.com

#
# Copyright 2021 Cuemacro Ltd. - http//www.cuemacro.com / @cuemacro
#
# See the License for the specific language governing permissions and limitations under the License.
#

if __name__ == '__main__':
    import time
    import vaex
    import pandas as pd

    import glob
    import os

    from findatapy.util.loggermanager import LoggerManager

    start = time.time()

    data_vendor = 'dukascopy' # 'ncfx' or 'dukascopy'

    source_folder = '/data/csv_dump/' + data_vendor + '/'
    destination_folder = '/data/csv_dump/' + data_vendor + '_arrow/'

    logger = LoggerManager().getLogger(__name__)

    parquet_list = glob.glob(source_folder + '/*.parquet')

    for p in parquet_list:
        df = pd.read_parquet(p)

        df = vaex.from_pandas(df, name='pandas', copy_index=True, index_name='Date')

        logger.info("Converting " + p + "...")
        filename = os.path.basename(p)

        df.export(destination_folder + "/" + filename.replace('parquet', 'arrow'))

    finish = time.time()
    print('Status: calculated ' + str(round(finish - start, 3)) + "s")
