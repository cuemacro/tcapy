"""Converts HDF5 files with DataFrames into CSV and dumps to disk
"""

from __future__ import print_function, division

__author__ = 'saeedamen'  # Saeed Amen / saeed@cuemacro.com

#
# Copyright 2017 Cuemacro Ltd. - http//www.cuemacro.com / @cuemacro
#
# See the License for the specific language governing permissions and limitations under the License.
#

from tcapy.util.utilfunc import UtilFunc

import glob

if __name__ == '__main__':

    REVERSE_SORT_CSV = False

    input_path = '/home/tcapyuser/cuemacro/tcapy/tests_harness_data'
    output_path = '/home/tcapyuser/cuemacro/tcapy/tests_harness_data'

    # Convert from H5 to CSV file (change input_path as appropriate) - change input_path as appropriate
    h5_file_list = [input_path + '/small_test_market_df.h5',
                    input_path + '/small_test_market_df_reverse.h5']

    for h5_file in h5_file_list:

        if '*' in h5_file:
            h5_mini_list = glob.glob(h5_file)
        else:
            h5_mini_list = [h5_file]

        for next_h5_file in h5_mini_list:
            df = UtilFunc().read_dataframe_from_binary(next_h5_file, format='hdf5')

            if REVERSE_SORT_CSV:
                df = df.sort_index(ascending=False)

            UtilFunc().write_dataframe_to_binary(df, next_h5_file.replace(input_path, output_path)
                                                 .replace('.h5', '.parquet'), format='parquet')