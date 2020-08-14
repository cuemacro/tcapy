"""Script to copy trade/order CSVs from disk and dump them into a SQL database. Note, that by default, they will replace
any existing tables (this can be changed to 'append')

Uses DataDumper underneath to access the various databases (via DatabaseSource)
"""

from __future__ import division, print_function

__author__ = 'saeedamen'  # Saeed Amen / saeed@cuemacro.com

#
# Copyright 2020 Cuemacro Ltd. - http//www.cuemacro.com / @cuemacro
#
# See the License for the specific language governing permissions and limitations under the License.
#

from tcapy.conf.constants import Constants
import os

constants = Constants()

if __name__ == '__main__':
    # 'ms_sql_server' or 'mysql' or 'sqlite'
    sql_database_type = 'mysql'
    trade_data_database_name = 'trade_database'
    trade_order_path = '/data/csv_dump/trade_order/'

    # Where are the trade/order CSVs stored, and how are they mapped?
    # This assumes you have already generated these files!

    # eg. 'trade' is the SQL table name, rather than the nickname we use
    csv_sql_table_trade_order_mapping = {'trade' : os.path.join(trade_order_path, 'trade_df_dump.csv'),
                                         'order' : os.path.join(trade_order_path, 'order_df_dump.csv')}

    # If no server server_host is specified then the default one from constants will be returned
    server_host = None

    # 'replace' or 'append' existing database table (replace will totally wipe it!)
    if_exists_trade_table = 'replace'

    from tcapy.data.datadumper import DataDumper

    data_dumper = DataDumper()
    data_dumper.upload_trade_data_flat_file(sql_database_type=sql_database_type, trade_data_database_name=trade_data_database_name,
                      csv_sql_table_trade_order_mapping=csv_sql_table_trade_order_mapping,
                      server_host=server_host, if_exists_trade_table=if_exists_trade_table)

