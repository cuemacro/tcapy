__author__ = 'saeedamen'  # Saeed Amen / saeed@cuemacro.com

#
# Copyright 2017 Cuemacro Ltd. - http//www.cuemacro.com / @cuemacro
#
# See the License for the specific language governing permissions and limitations under the License.
#

"""Has the configuration settings for celery. The main thing that needs to be changed is the broker URL settings (in
the ConstantsGen file"
"""

from tcapy.conf.constants import Constants

constants = Constants()

BROKER_URL = constants.celery_broker_url
CELERY_RESULT_BACKEND = constants.celery_result_backend

# from kombu import serialization
# serialization.registry._decoders.("application/x-python-serialize")

# the below should not need to be changed by nearly all users
# CELERY_RESULT_BACKEND = "amqp"
# CELERY_RESULT_BACKEND = "redis://localhost:6379/2"
CELERY_EVENT_SERIALIZER = 'pickle'
CELERY_ACCEPT_CONTENT = ['pickle'] #
CELERY_TASK_SERIALIZER = 'pickle'
CELERY_RESULT_SERIALIZER = 'pickle'
CELERY_HIJACK_ROOT_LOGGER = False
CELERY_STORE_ERRORS_EVEN_IF_IGNORED = True
CELERYD_MAX_TASKS_PER_CHILD = 50 # Stop memory leaks, so restart workers after a 100 tasks
CELERY_ACKS_LATE = True
CELERY_TASK_RESULT_EXPIRES = 300 # Clear memory after a while of results, if not picked up
# CELERY_ALWAYS_EAGER = True # For debugging
BROKER_TRANSPORT_OPTIONS = {'socket_timeout': 300}
# BROKER_POOL_LIMIT = 0
