#
# This file is licensed under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from smv.error import SmvRuntimeError
from smv.smvhdfs import SmvHDFS
from datetime import datetime
import time

class SmvLock(object):
    """Create a lock context
    """
    def __init__(self, _jvm, lock_path, timeout = 3600000):
        self._jvm = _jvm
        self.lock_path = lock_path
        self.timeout = timeout

        self._obtained = False
        self._attempts = 0

    def __enter__(self):
        self.lock()
        return None

    def lock(self):
        if (self._obtained):
            raise SmvRuntimeError("Non-reentrant lock already obtained")

        start = datetime.now()
        while (not self._obtained):
            self._attempts = self._attempts + 1
            try:
                SmvHDFS(self._jvm).createFileAtomic(self.lock_path)
                self._obtained = True
            except:
                if (datetime.now() - start).total_seconds() * 1000 > self.timeout:
                    raise TimeoutError("Cannot obtain lock [{}] within {} seconds".format(self.lock_path, self.timeout / 1000))
                if (self._attempts == 1):
                    print("Found existing lock file [{}]".format(self.lock_path))
                
                try:
                    time.sleep(10)      # sleep 10 second before next check
                except KeyboardInterrupt:
                    pass                # getting waken up is okay, check if lock is available again

    def unlock(self):
        SmvHDFS(self._jvm).deleteFile(self.lock_path)

    def __exit__(self, type, value, traceback):
        self.unlock()

        # Reset counter
        self._obtained = False
        self._attempts = 0


class NonOpLock(object):
    def __enter__(self):
        pass
    def __exit__(self, type, value, traceback):
        pass