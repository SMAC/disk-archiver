from zope.interface import implements
from twisted.internet import defer
from smac.python import log
from smac.conf import settings
from smac.api.archiver.ttypes import InvalidChecksum
from smac.api.archiver import FileReceiver
from smac.amqp.models import Address
from smac.tasks import Task

import hashlib
import time
import tempfile
import os

def sizeof_fmt(num):
    for x in ['bytes', 'KB', 'MB', 'GB', 'TB']:
        if num < 1024.0:
            return "%3.1f %s" % (num, x)
        num /= 1024.0

class FileReceiver(Task):
    """
    
    """
    
    implements(FileReceiver.Iface)
    
    bufsize = 2 ** 24 # 16 MB
    
    def __init__(self, transfer_key, path, size, parent='', hashmethod=hashlib.sha512):
        super(FileReceiver, self).__init__(parent=parent)
        
        self.transfer_key = transfer_key
        self.path = path
        fd, temp = tempfile.mkstemp()
        self.tempfile = os.fdopen(fd, 'w+b', self.bufsize), temp
        self.start_time = time.time()
        self.size = size
        self.remaining_size = size
        self.received = 0
        self.checksum = hashmethod()
        self.deferred_completed = defer.Deferred()
        
        log.info("Initializing transfer with key '{0}'".format(transfer_key))
        log.debug(" - Temporary file: {0}".format(self.tempfile[1]))
        log.debug(" - Transfer size:  {0}".format(sizeof_fmt(size)))
        log.debug(" - Final path:     {0}".format(path))
        log.debug(" - Hash method:    {0}".format(hashmethod.__name__))
        
        self.start()
    
    def run(self):
        pass
    
    def cleanup(self):
        self.tempfile[0].close()
        
        if os.path.exists(self.tempfile[1]):
            os.remove(self.tempfile[1])
    
    @defer.inlineCallbacks
    def finish(self, checksum):
        # Wait for the transfer to complete
        yield self.deferred_completed
        
        # Close transfer queues and remove bindings
        # @todo: Cleanup
        
        # Close the file and move it to the final destination
        self.tempfile[0].close()
        os.rename(self.tempfile[1], self.path)
        
        self.duration = time.time() - self.start_time
        error = self.checksum.hexdigest() != checksum
        
        if error:
            log.error("Transfer with key '{0}' failed".format(self.transfer_key))
            log.debug(" - Checksums:     source {0}".format(checksum))
            log.debug("                  receiv {0}".format(self.checksum.hexdigest()))
        else:
            log.info("Transfer with key '{0}' successfully completed".format(self.transfer_key))
            log.debug(" - Checksum:      {0}".format(checksum))
        
        log.debug(" - Duration:      {0:.1f} s".format(self.duration))
        log.debug(" - Average speed: {0}/s".format(sizeof_fmt(self.size / self.duration)))
        
        if error:
            raise InvalidChecksum(self.transfer_key)
    
    def send_data_chunk(self, data):
        self.tempfile[0].write(data)
        self.checksum.update(data)
        length = len(data)
        self.received += length
        self.remaining_size -= length
        self.completed = self.received * 1.0 / self.size
        
        if not self.received % (1024 * 1024 * 5):
            self.status_text = "Receiving {0} ({1} remaining)".format(self.path, sizeof_fmt(self.remaining_size))
        
        if self.remaining_size <= 0:
            self.complete()
            self.deferred_completed.callback(None)
        
    

