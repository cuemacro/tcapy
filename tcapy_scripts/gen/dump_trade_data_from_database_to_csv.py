"""Script to fetch trade/order data from database (here from a defined SQL database) and dump to disk as a CSV file.
This can be useful if we want to transfer the data to another computer later, or for backup purposes.
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
    # 'ms_sql_server' or 'mysql'
    sql_dialect = 'ms_sql_server'
    trade_data_database_name = 'trade_database'

    trade_data_folder = ''

    if sql_dialect == 'ms_sql_server':
        from tcapy.data.databasesource import DatabaseSourceMSSQLServer as DatabaseSource
    elif sql_dialect == 'mysql':
        from tcapy.data.databasesource import DatabaseSourceMySQL as DatabaseSource

    # Where to dump the CSV (& Parquet) files
    csv_trade_order_mapping_dump = {'trade_df' : trade_data_folder + 'trade_df_dump.csv',
                                    'order_df' : trade_data_folder + 'order_df_dump.csv'}

    # Get the actual table names in the database which may differ from "nicknames"
    trade_order_mapping = constants.trade_order_mapping[sql_dialect]

    database_source = DatabaseSource(trade_data_database_name=trade_data_database_name)

    # Go through each trade/order and then dump as CSV and Parquet files on disk
    for k in trade_order_mapping.keys():
        trade_order_df = database_source.fetch_trade_order_data(table_name=trade_order_mapping[k])
        trade_order_df.to_csv(csv_trade_order_mapping_dump[k])
        trade_order_df.to_parquet(csv_trade_order_mapping_dump[k].replace('csv', 'parquet'))

