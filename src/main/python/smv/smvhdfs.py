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

from smv.utils import is_string

class SmvHDFS(object):
    def __init__(self, _jvm):
        self.URI = _jvm.java.net.URI
        self.Path = _jvm.org.apache.hadoop.fs.Path
        self.FileSystem = _jvm.org.apache.hadoop.fs.FileSystem
        self.hadoopConf = _jvm.org.apache.hadoop.conf.Configuration()
        self.StringWriter = _jvm.java.io.StringWriter
        self.IOUtils = _jvm.org.apache.commons.io.IOUtils
        self.String = _jvm.java.lang.String

    def _getFileSystem(self, path):
        uri = self.URI(path)
        return self.FileSystem.get(uri, self.hadoopConf)

    def createFileAtomic(self, fileName):
        self._getFileSystem(fileName).create(self.Path(fileName), False).close()
        return None

    def deleteFile(self, fileName):
        self._getFileSystem(fileName).delete(self.Path(fileName), True)
        return None

    def exists(self, fileName): 
        return self._getFileSystem(fileName).exists(self.Path(fileName))

    def readFromFile(self, fileName):
        path = self.Path(fileName)
        hdfs = self._getFileSystem(fileName)

        stream = hdfs.open(path)
        writer = self.StringWriter()
        self.IOUtils.copy(stream, writer, "UTF-8")
        return writer.toString()

    def writeToFile(self, source, file_name):
        if is_string(source):
            writer = self._openForWrite(file_name)
            writer.write(bytearray(source, "utf-8"))
            writer.close()
        else:
            self._writeStreamToFile(source, file_name)

    def dirList(self, dirName):
        try:
            path = self.Path(dirName)
            hdfs = self._getFileSystem(dirName)
            status = hdfs.listStatus(path)
            names = [str(s.getPath().getName()) for s in status]
        except:
            names = []

        return names

    def modificationTime(self, fileName):
        if "*" in fileName:
            mt = 0
        else:
            path = self.Path(fileName)
            hdfs = self._getFileSystem(fileName)

            mt = hdfs.getFileStatus(path).getModificationTime()

        return mt

    def _openForWrite(self, fileName): 
        path = self.Path(fileName)
        hdfs = self._getFileSystem(fileName)

        if (hdfs.exists(path)):
            hdfs.delete(path, True)

        return hdfs.create(path)
     
    def _writeStreamToFile(self, py_fileobj, file_name):
        out = self._openForWrite(file_name)
        maxsize = 8192
        def read():
            buf = py_fileobj.read(maxsize)
            # The following should work in both Python 2.7 and 3.5.
            #
            # In 2.7, read() returns a str even in 'rb' mode, but calling
            # bytearray converts it to the right type.
            #
            # In 3.5, read() returns a bytes in 'rb' mode, and calling
            # bytearray does not require a specified encoding
            buf = bytearray(buf)
            return buf

        try:
            buf = read()
            while (len(buf) > 0):
                out.write(buf, 0, len(buf))
                buf = read()
        finally:
            out.close()
            py_fileobj.close()
        

 