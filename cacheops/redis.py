from __future__ import absolute_import

import os.path
import random
import re
import six
import socket
import sys
import traceback
import warnings
from copy import copy

import redis
from django.core.exceptions import ImproperlyConfigured
from django.utils.module_loading import import_string
from funcy import decorator, identity, memoize

from .conf import settings

handle_connection_failure = identity

client_class_name = getattr(settings, 'CACHEOPS_CLIENT_CLASS', None)
client_class = import_string(client_class_name) if client_class_name else redis.StrictRedis


def ip(hostname):
    try:
        return socket.gethostbyname(hostname)
    except socket.gaierror as err:
        warnings.warn('Hostname %s did not resolve because %s' % (hostname, err))
        raise


if settings.CACHEOPS_DEGRADE_ON_FAILURE:

    @decorator
    def handle_connection_failure(call):
        try:
            return call()
        except redis.ConnectionError as e:
            warnings.warn("The cacheops cache is unreachable! Error: %s" % e, RuntimeWarning)
        except redis.TimeoutError as e:
            warnings.warn("The cacheops cache timed out! Error: %s" % e, RuntimeWarning)
        except Exception as e:
            warnings.warn("".join(traceback.format_exception(*sys.exc_info())))

    class SafeRedis(client_class):
        get = handle_connection_failure(client_class.get)

    client_class = SafeRedis


if settings.CACHEOPS_REDIS_REPLICAS:

    read_client_class = copy(client_class)

    class ReplicaProxyRedis(client_class):

        read_clients = []

        @classmethod
        def set_read_clients(cls):
            primary_ip = ip(settings.CACHEOPS_REDIS['host'])
            replicas = settings.CACHEOPS_REDIS_REPLICAS
            replica_weight = settings.CACHEOPS_REPLICA_WEIGHT

            # Make Redis clients from all the URLs except the primary
            new_clients = [read_client_class(**r) for r in replicas if ip(r['host']) != primary_ip]

            # Duplicate each client a few times if desired
            if replica_weight > 1:
                new_clients = [c for c in new_clients for _ in range(replica_weight)]

            # Add just one Redis client for the primary, if it was in the replicas
            if len(new_clients) < len(replicas):
                new_clients.append(read_client_class(**settings.CACHEOPS_REDIS))

            cls.read_clients = new_clients

        def get(self, *args, **kwargs):
            """Proxy `get` calls to redis replica."""
            if not self.read_clients:
                self.set_read_clients()
            try:
                client = random.choice(self.read_clients)
                return client.get(*args, **kwargs)
            except redis.ConnectionError:
                return super(ReplicaProxyRedis, self).get(*args, **kwargs)

        def execute_command(self, *args, **options):
            """Handle failover of AWS elasticache."""
            try:
                return super(ReplicaProxyRedis, self).execute_command(*args, **options)
            except redis.ResponseError as e:
                if "READONLY" not in e.message:
                    raise
                connection = self.connection_pool.get_connection(args[0], **options)
                connection.disconnect()
                self.read_clients = []
                warnings.warn("Primary probably failed over, reconnecting")
                return super(ReplicaProxyRedis, self).execute_command(*args, **options)

    client_class = ReplicaProxyRedis

if isinstance(settings.CACHEOPS_REDIS, six.string_types):
    redis_client = client_class.from_url(settings.CACHEOPS_REDIS)
else:
    redis_client = client_class(**settings.CACHEOPS_REDIS)


# Lua script loader

STRIP_RE = re.compile(r'TOSTRIP.*/TOSTRIP', re.S)


@memoize
def load_script(name, strip=False):
    filename = os.path.join(os.path.dirname(__file__), 'lua/%s.lua' % name)
    with open(filename) as f:
        code = f.read()
    if strip:
        code = STRIP_RE.sub('', code)
    return redis_client.register_script(code)
