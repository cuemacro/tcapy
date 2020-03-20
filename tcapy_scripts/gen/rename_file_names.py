"""Renames filenames in a folder.
"""
from __future__ import print_function, division

__author__ = 'saeedamen'  # Saeed Amen / saeed@cuemacro.com

#
# Copyright 2017 Cuemacro Ltd. - http//www.cuemacro.com / @cuemacro
#
# See the License for the specific language governing permissions and limitations under the License.
#

if __name__ == '__main__':

    import os

    add_vendor = 'dukascopy'

    path = '/home/redhat/tcapy_tests_data/csv_dump'

    filenames = os.listdir(path)

    for filename in filenames:
        os.rename(path + "/" + filename, path + "/" + filename.replace("USDJPY", "USDJPY_" + add_vendor + "_"))
        # os.rename(path + "/" + filename, path + "/" + filename.replace("large_", ""))