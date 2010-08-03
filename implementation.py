# Copyright (C) 2005-2010  MISG/ICTI/EIA-FR
# See LICENSE for details.

"""
A simple archiver implementation which writes files to a given directory to
disk.

To run this module, the settings file should contain an additional 'data_root'
directive on the top level::

    {
        "data_root": "/Users/garetjax/temp/smac_archiver_data"
    }

@author: Jonathan Stoppani <jonathan.stoppani@edu.hefr.ch>
"""


import os

from zope.interface import implements
from twisted.internet import defer

from smac.python import log

from smac.amqp.models import Address
from smac.api.archiver import FileReceiver as ThriftReceiver
from smac.api.archiver.Archiver import Iface
from smac.api.archiver.ttypes import AlreadyUploading
from smac.conf import settings
from smac.conf.topology import queue, binding
from smac.modules import base

from receiver import FileReceiver


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
        """
        Starts a FileReceiver server for the given C{key}.
        """
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
        """
        Waits for the FileReceiver server to succeed and return its response
        to the caller.
        
        @todo: Requests for inexistent uploads are ignored, raise an exception
               instead.
        """
        try:
            return self.uploads.pop(key).finish(checksum)
        except KeyError:
            log.warn("Upload with key {0} not found. Can't finalize".format(key))
            return

