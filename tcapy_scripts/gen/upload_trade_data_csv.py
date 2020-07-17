"""Script to copy trade/order CSVs from disk and dump them into a SQL database. Note, that by default, they will replace
any existing tables (this can be changed to 'append')

"""

from __future__ import division, print_function

__author__ = 'saeedamen'  # Saeed Amen / saeed@cuemacro.com

#
# Copyright 2020 Cuemacro Ltd. - http//www.cuemacro.com / @cuemacro
#
# See the License for the specific language governing permissions and limitations under the License.
#

from tcapy.conf.constants import Constants

constants = Constants()

if __name__ == '__main__':
    # 'ms_sql_server' or 'mysql' or 'sqlite'
    sql_database = 'mysql'
    trade_data_database_name = 'trade_database'

    if sql_database == 'ms_sql_server':
        from tcapy.data.databasesource import DatabaseSourceMSSQLServer as DatabaseSource
    elif sql_database == 'mysql':
        from tcapy.data.databasesource import DatabaseSourceMySQL as DatabaseSource
    elif sql_database == 'sqlite':
        from tcapy.data.databasesource import DatabaseSourceSQLite as DatabaseSource

    # 'replace' or 'append'
    if_exists_trade_table = 'replace'

    # Where are the trade/order CSVs stored, and how are they mapped?
    # This assumes you have already generated these files!
    csv_sql_table_trade_order_mapping = {'trade' : 'trade_df_dump.csv', 'order' : 'order_df_dump.csv'}

    # Get the actual table names in the database which may differ from "nicknames"
    trade_order_mapping = constants.trade_order_mapping[sql_database]

    database_source = DatabaseSource(trade_data_database_name=trade_data_database_name, server_host='localhost')

    for key in csv_sql_table_trade_order_mapping.keys():
        database_source.convert_csv_to_table(
            csv_sql_table_trade_order_mapping[key], None, key, database_name=trade_data_database_name,
            if_exists_table=if_exists_trade_table)
