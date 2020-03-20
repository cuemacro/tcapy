__author__ = 'saeedamen'  # Saeed Amen / saeed@cuemacro.com

#
# Copyright 2017 Cuemacro Ltd. - http//www.cuemacro.com / @cuemacro
#
# See the License for the specific language governing permissions and limitations under the License.
#

from tcapy.conf.constants import Constants

constants = Constants()

class Swim(object):
    """Creating thread and process pools in a generic way. Allows users to specify the underlying thread or multiprocess library
    they wish to use. Note you can share Pool objects between processes.

    """

    def __init__(self, parallel_library=None):
        self._pool = None

        if parallel_library is None:
            parallel_library = constants.parallel_library

        self._parallel_library = parallel_library

        if parallel_library == 'multiprocess':
            try:
                import multiprocess;
                multiprocess.freeze_support()
            except:
                pass
        elif parallel_library == 'pathos':
            try:
                import pathos
                pathos.helpers.freeze_support()
            except:
                pass

    def create_pool(self, thread_no=constants.thread_no, force_new=True):

        if not (force_new) and self._pool is not None:
            return self._pool

        if self._parallel_library == "thread":
            from multiprocessing.dummy import Pool
        elif self._parallel_library == 'multiprocess':
            from multiprocess import Pool
        elif self._parallel_library == 'pathos':
            from pathos.pools import ProcessPool as Pool

        if thread_no == 0:
            self._pool = Pool()
        else:
            self._pool = Pool(thread_no)

        return self._pool

    def close_pool(self, pool, force_process_respawn=False):
        if constants.parallel_library != 'pathos' or force_process_respawn and pool is not None:
            pool.close()
            pool.join()
