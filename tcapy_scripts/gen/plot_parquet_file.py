"""Plots Parquet files, such as those which have been downloaded from Dukascopy
"""

from __future__ import print_function, division

__author__ = 'saeedamen'  # Saeed Amen / saeed@cuemacro.com

#
# Copyright 2020 Cuemacro
#
# See the License for the specific language governing permissions and limitations under the License.
#

import os

from chartpy import Chart
from tcapy.data.databasesource import DatabaseSourceCSVBinary

parquet_path = '/data/csv_dump/dukascopy/'

filename = ['EURUSD_dukascopy_2020-05-01_00_00_00.096000+00_002020-05-31_23_59_59.084000+00_00.parquet']

for f in filename:
    final_path = os.path.join(parquet_path, f)

    database_source = DatabaseSourceCSVBinary(market_data_database_csv=final_path)
    df = database_source.fetch_market_data()

    print(df)

    df_resample = df.resample('1min').last()

    Chart().plot(df_resample)
