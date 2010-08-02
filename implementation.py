from zope.interface import implements
from twisted.internet import defer
from smac.python import log
from smac.conf import settings
from smac.api.archiver import FileReceiver as ThriftReceiver
from smac.api.archiver.Archiver import Iface
from smac.api.archiver.ttypes import AlreadyUploading
from smac.modules import base
from smac.util.hooks import register
from receiver import FileReceiver
import os
from smac.conf.topology import queue, binding
from smac.amqp.models import Address

class SimpleDiskArchiver(base.ModuleBase):
    implements(Iface)
    
    def __init__(self, *args, **kwargs):
        super(SimpleDiskArchiver, self).__init__(*args, **kwargs)
        
        self.uploads = {}
        """Register of all currently running uploads"""
        
        self.root = os.path.realpath(settings.data_root)
        """Base directory where all files are contained"""
        
        log.info("Serving files from {0}".format(self.root))
    
    def amq_stop(self):
        for v in self.uploads.values():
            v.cleanup()
    
    @defer.inlineCallbacks
    def start_upload(self, key, path, size, parent):
        if key in self.uploads:
            log.warn("Can't start another upload for {0}: already uploading".format(key))
            raise AlreadyUploading(key)
        
        path = os.path.join(self.root, key)
        
        handler = FileReceiver(key, path, size, parent)
        self.task_register.add(handler)
        address = Address(routing_key=key)
        
        queues = (
            queue('', (
                binding('transfers', '{routing_key}'),
            ), extra={
                'exclusive': True,
                'auto_delete': True,
            }),
        )
        
        self.uploads[key] = handler
        yield self.amq_server(address, ThriftReceiver, handler, queues)
    
    def finalize_upload(self, key, checksum):
        try:
            return self.uploads.pop(key).finish(checksum)
        except KeyError:
            log.warn("Upload with key {0} not found. Can't finalize".format(key))
            return

