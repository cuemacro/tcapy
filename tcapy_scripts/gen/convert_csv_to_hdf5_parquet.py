"""Converts CSV files with DataFrames into Parquet (or HDF5) and dumps to disk
"""

from __future__ import print_function, division

__author__ = 'saeedamen'  # Saeed Amen / saeed@cuemacro.com

#
# Copyright 2017 Cuemacro Ltd. - http//www.cuemacro.com / @cuemacro
#
# See the License for the specific language governing permissions and limitations under the License.
#

from tcapy.util.utilfunc import UtilFunc
from tcapy.conf.constants import Constants

import os

constants = Constants()

if __name__ == '__main__':

    folder = constants.test_data_harness_folder

    csv_market_data_files = ['small_test_market_df.csv.gz', 'small_test_market_df_reverse.csv.gz']

    # Can either dump to Parquet (default) or HDF (optional)
    # format = 'hdf5'; file_ext = 'h5'
    format = 'parquet'; file_ext = 'parquet'

    for csv_market_data in csv_market_data_files:
        csv_market_data = os.path.join(folder, csv_market_data)

        REVERSE_SORT = False

        from tcapy.data.databasesource import DatabaseSourceCSV

        # Read CSV and parse the main field
        df = DatabaseSourceCSV()._fetch_table(csv_market_data)

        if REVERSE_SORT:
            df = df.sort_index(ascending=False)

        h5_market_data = csv_market_data.replace('.csv.gz', '.' + file_ext)
        UtilFunc().write_dataframe_to_binary(df, h5_market_data, format=format)