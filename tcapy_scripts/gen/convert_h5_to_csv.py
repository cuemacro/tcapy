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

if __name__ == '__main__':

    REVERSE_SORT_CSV = False

    # Convert from H5 to CSV file (change path as appropriate) - change path as appropriate
    h5_file = '/mnt/e/Remote/tcapy/tests_harness_data/test_market_EURUSD.h5'

    df = UtilFunc().read_dataframe_from_binary(h5_file)

    if REVERSE_SORT_CSV:
        df = df.sort_index(ascending=False)

    df.to_csv(h5_file.replace('.h5', '.csv'))