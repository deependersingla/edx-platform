"""
https://github.com/arngarden/MongoDBProxy

Copyright 2013 Gustav Arngarden

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""

import time
import pymongo

MONGO_METHODS_NEEDING_RETRY = {
    pymongo.collection.Collection: [
        'find', 'ensure_index', 'aggregate', 'parallel_scan', 'group', 'map_reduce', 'inline_map_reduce'
    ],
}


class Executable:
    """
    Wrap a MongoDB-method and handle AutoReconnect-exceptions.
    """

    def __init__(self, method, logger, wait_time=1):
        self.method = method
        self.logger = logger
        self.wait_time = wait_time

    def __call__(self, *args, **kwargs):
        """Automatic handling of AutoReconnect-exceptions."""
        start = time.time()
        i = 0
        while True:
            try:
                return self.method(*args, **kwargs)
            except pymongo.errors.AutoReconnect:
                end = time.time()
                delta = end - start
                if delta >= self.wait_time:
                    break
                self.logger.warning('AutoReconnecting, try %d (%.1f seconds)'
                                    % (i, delta))
                time.sleep((self.wait_time / 6) * i)
                i += 1
        # Try one more time, but this time, if it fails, let the
        # exception bubble up to the caller.
        return self.method(*args, **kwargs)

    def __dir__(self):
        return dir(self.method)

    def __str__(self):
        return self.method.__str__()

    def __repr__(self):
        return self.method.__repr__()


class MongoProxy:
    """
    Proxy for MongoDB connection.
    Methods that are executable, i.e find, insert etc, get wrapped in an
    Executable-instance that handles AutoReconnect-exceptions transparently.
    """
    def __init__(self, proxied_object, logger=None, wait_time=None):
        """
        proxied_object is an ordinary MongoDB-connection.
        """
        if logger is None:
            import logging
            logger = logging.getLogger(__name__)

        self.proxied_object = proxied_object
        self.logger = logger
        self.wait_time = wait_time

    def __getitem__(self, key):
        """
        Create and return proxy around attribute "key" if it is a method.
        Otherwise just return the attribute.
        """
        item = self.proxied_object[key]
        if hasattr(item, '__call__'):
            return MongoProxy(item, self.logger, self.wait_time)
        return item

    def __setitem__(self, key, value):
        self.proxied_object[key] = value

    def __delitem__(self, key):
        del self.proxied_object[key]

    def __len__(self):
        return len(self.proxied_object)

    def __getattr__(self, key):
        """
        If key is the name of an executable method in the MongoDB connection,
        for instance find or insert, wrap this method in Executable-class that
        handles AutoReconnect-Exception.
        """
        attr = getattr(self.proxied_object, key)
        if hasattr(attr, '__call__'):
            attributes_for_class = MONGO_METHODS_NEEDING_RETRY.get(self.proxied_object.__class__, [])
            if key in attributes_for_class:
                return Executable(attr, self.logger, self.wait_time)
            else:
                return MongoProxy(attr, self.logger, self.wait_time)
        return attr

    def __call__(self, *args, **kwargs):
        return self.proxied_object(*args, **kwargs)

    def __dir__(self):
        return dir(self.proxied_object)

    def __str__(self):
        return self.proxied_object.__str__()

    def __repr__(self):
        return self.proxied_object.__repr__()

    def __nonzero__(self):
        return True
