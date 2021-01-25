from __future__ import division
from __future__ import print_function

__author__ = 'saeedamen'  # Saeed Amen / saeed@cuemacro.com

#
# Copyright 2019 Cuemacro Ltd. - http//www.cuemacro.com / @cuemacro
#
# See the License for the specific language governing permissions and limitations under the License.
#

from tcapy.conf.constants import Constants

constants = Constants()

class AccessControl(object):
    """The AccessControl object has username/password combinations for accessing protected resources (eg. databases, or external
    data sources) which can be set by the user. It will default to those in the constants file (these can be set to None to force
    users to create their own AccessControl objects when making database requests)

    """

    def __init__(self,

                 # trade/order data
                 ms_sql_server_username=constants.ms_sql_server_username,  ms_sql_server_password=constants.ms_sql_server_password,
                 postgres_username=constants.postgres_username, postgres_password=constants.postgres_password,
                 mysql_username=constants.mysql_username, mysql_password=constants.mysql_password,

                 # market data
                 arctic_username=constants.arctic_username, arctic_password=constants.arctic_password,
                 influxdb_username=constants.influxdb_username, influxdb_password=constants.influxdb_password,
                 questdb_username=constants.questdb_username, questdb_password=constants.questdb_password,
                 kdb_username=constants.kdb_username, kdb_password=constants.kdb_password,
                 clickhouse_username=constants.clickhouse_username, clickhouse_password=constants.clickhouse_password,

                 # external sources
                 ncfx_username=constants.ncfx_username, ncfx_password=constants.ncfx_password, ncfx_url=constants.ncfx_url,

                 eikon_api_key=constants.eikon_api_key
                 ):

        self.ms_sql_server_username = ms_sql_server_username
        self.ms_sql_server_password = ms_sql_server_password
        self.postgres_username = postgres_username
        self.postgres_password = postgres_password
        self.mysql_username = mysql_username
        self.mysql_password = mysql_password

        self.arctic_username = arctic_username
        self.arctic_password = arctic_password
        self.influxdb_username = influxdb_username
        self.influxdb_password = influxdb_password
        self.questdb_username = questdb_username
        self.questdb_password = questdb_password
        self.kdb_username = kdb_username
        self.kdb_password = kdb_password
        self.clickhouse_username = clickhouse_username
        self.clickhouse_password = clickhouse_password

        self.ncfx_username = ncfx_username
        self.ncfx_password = ncfx_password
        self.ncfx_url = ncfx_url

        self.eikon_api_key = eikon_api_key
    
    # trade/order data username/password
    @property
    def ms_sql_server_username(self):
        return self.__ms_sql_server_username

    @ms_sql_server_username.setter
    def ms_sql_server_username(self, ms_sql_server_username):
        self.__ms_sql_server_username = ms_sql_server_username
        
    @property
    def ms_sql_server_password(self):
        return self.__ms_sql_server_password

    @ms_sql_server_password.setter
    def ms_sql_server_password(self, ms_sql_server_password):
        self.__ms_sql_server_password = ms_sql_server_password

    @property
    def postgres_username(self):
        return self.__postgres_username

    @postgres_username.setter
    def postgres_username(self, postgres_username):
        self.__postgres_username = postgres_username
        
    @property
    def postgres_password(self):
        return self.__postgres_password

    @postgres_password.setter
    def postgres_password(self, postgres_password):
        self.__postgres_password = postgres_password
    
    @property
    def mysql_username(self):
        return self.__mysql_username

    @mysql_username.setter
    def mysql_username(self, mysql_username):
        self.__mysql_username = mysql_username
        
    @property
    def mysql_password(self):
        return self.__mysql_password

    @mysql_password.setter
    def mysql_password(self, mysql_password):
        self.__mysql_password = mysql_password
        
    
        
    # market data username/password
    @property
    def arctic_username(self):
        return self.__arctic_username

    @arctic_username.setter
    def arctic_username(self, arctic_username):
        self.__arctic_username = arctic_username

    @property
    def arctic_password(self):
        return self.__arctic_password

    @arctic_password.setter
    def arctic_password(self, arctic_password):
        self.__arctic_password = arctic_password
        
    @property
    def influxdb_username(self):
        return self.__influxdb_username

    @influxdb_username.setter
    def influxdb_username(self, influxdb_username):
        self.__influxdb_username = influxdb_username

    @property
    def influxdb_password(self):
        return self.__influxdb_password

    @influxdb_password.setter
    def influxdb_password(self, influxdb_password):
        self.__influxdb_password = influxdb_password

    @property
    def questdb_username(self):
        return self.__questdb_username

    @questdb_username.setter
    def questdb_username(self, questdb_username):
        self.__questdb_username = questdb_username

    @property
    def questdb_password(self):
        return self.__questdb_password

    @questdb_password.setter
    def questdb_password(self, questdb_password):
        self.__questdb_password = questdb_password
        
    @property
    def kdb_username(self):
        return self.__kdb_username

    @kdb_username.setter
    def kdb_username(self, kdb_username):
        self.__kdb_username = kdb_username

    @property
    def kdb_password(self):
        return self.__kdb_password

    @kdb_password.setter
    def kdb_password(self, kdb_password):
        self.__kdb_password = kdb_password

    @property
    def clickhouse_username(self):
        return self.__clickhouse_username

    @clickhouse_username.setter
    def clickhouse_username(self, clickhouse_username):
        self.__clickhouse_username = clickhouse_username

    @property
    def clickhouse_password(self):
        return self.__clickhouse_password

    @clickhouse_password.setter
    def clickhouse_password(self, clickhouse_password):
        self.__clickhouse_password = clickhouse_password
        

    @property
    def ncfx_username(self):
        return self.__ncfx_username

    @ncfx_username.setter
    def ncfx_username(self, ncfx_username):
        self.__ncfx_username = ncfx_username

    @property
    def ncfx_password(self):
        return self.__ncfx_password

    @ncfx_password.setter
    def ncfx_password(self, ncfx_password):
        self.__ncfx_password = ncfx_password
        
    @property
    def ncfx_url(self):
        return self.__ncfx_url

    @ncfx_url.setter
    def ncfx_url(self, ncfx_url):
        self.__ncfx_url = ncfx_url

    @property
    def eikon_api_key(self):
        return self.__eikon_api_key

    @eikon_api_key.setter
    def eikon_api_key(self, eikon_api_key):
        self.__eikon_api_key = eikon_api_key
