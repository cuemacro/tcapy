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

broker_url = constants.celery_broker_url
result_backend = constants.celery_result_backend

# from kombu import serialization
# serialization.registry._decoders.("application/x-python-serialize")

# the below should not need to be changed by nearly all users
# result_backend = "amqp"
# result_backend  = "redis://localhost:6379/2"
event_serializer = 'pickle'
accept_content = ['pickle'] #
task_serializer = 'pickle'
result_serializer = 'pickle'
worker_hijack_root_logger = False
task_store_errors_even_if_ignored = True
worker_max_tasks_per_child = 50 # Stop memory leaks, so restart workers after a 100 tasks
tasks_acks_late = True
result_expires = 900 # Clear memory after a while of results, if not picked up
# task_always_eager = True # For debugging, to run Celery in the same process
broker_transport_options = {'socket_timeout': 900}
# broker_pool_limit = 0
