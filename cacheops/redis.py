from __future__ import absolute_import
import warnings
import six
import socket
import sys
import traceback
from urlparse import urlparse

from funcy import decorator, identity, memoize
import redis
from django.core.exceptions import ImproperlyConfigured
from django.utils.module_loading import import_string
import random

from .conf import settings

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
else:
    handle_connection_failure = identity

client_class_name = getattr(settings, 'CACHEOPS_CLIENT_CLASS', None)
client_class = import_string(client_class_name) if client_class_name else redis.StrictRedis
redis_replicas = []


def ip(url):
    try:
        return socket.gethostbyname(urlparse(url).hostname)
    except socket.gaierror:
        warnings.warn('Hostname in URL %s did not resolve' % url)
        raise


def set_redis_replicas():
    # the conf could be a list or string
    # list would look like: ["redis://cache-001:6379/1", "redis://cache-002:6379/2"]
    # string would be: "redis://cache-001:6379/1,redis://cache-002:6379/2"
    primary_url = settings.REDIS_MASTER
    primary_ip = ip(primary_url)
    redis_urls = settings.REDIS_REPLICAS
    if isinstance(redis_urls, six.string_types):
        redis_urls = redis_urls.split(',')
    else:
        redis_urls = list(redis_urls)
    replica_weight = settings.REDIS_REPLICA_WEIGHT

    # Make Redis clients from all the URLs except the primary
    new_read_clients = [redis.StrictRedis.from_url(u) for u in redis_urls if ip(u) != primary_ip]

    # Duplicate each client a few times if desired
    if replica_weight > 1:
        new_read_clients = [c for c in new_read_clients for _ in range(replica_weight)]

    # Add just one Redis client for the primary
    new_read_clients.append(redis.StrictRedis.from_url(primary_url))

    global redis_replicas
    redis_replicas = new_read_clients


class SafeRedis(client_class):
    get = handle_connection_failure(redis.StrictRedis.get)

    def execute_command(self, *args, **options):
        """ Handle failover of AWS elasticache."""
        try:
            return super(SafeRedis, self).execute_command(*args, **options)
        except redis.ResponseError as e:
            if "READONLY" not in e.message:
                raise
            connection = self.connection_pool.get_connection(args[0], **options)
            connection.disconnect()
            set_redis_replicas()
            warnings.warn("Primary probably failed over, reconnecting")
            return super(SafeRedis, self).execute_command(*args, **options)


class LazyRedis(object):
    def _setup(self):
        Redis = SafeRedis if settings.CACHEOPS_DEGRADE_ON_FAILURE else redis.StrictRedis

        # Allow client connection settings to be specified by a URL.
        if isinstance(settings.CACHEOPS_REDIS, six.string_types):
            client = Redis.from_url(settings.CACHEOPS_REDIS)
        else:
            client = Redis(**settings.CACHEOPS_REDIS)

        object.__setattr__(self, '__class__', client.__class__)
        object.__setattr__(self, '__dict__', client.__dict__)

    def __getattr__(self, name):
        self._setup()
        return getattr(self, name)

    def __setattr__(self, name, value):
        self._setup()
        return setattr(self, name, value)

CacheopsRedis = SafeRedis if settings.CACHEOPS_DEGRADE_ON_FAILURE else client_class
try:
    set_redis_replicas()
except AttributeError as err:
    redis_client = LazyRedis()
else:
    class ReplicaProxyRedis(CacheopsRedis):
        """Proxy `get` calls to redis replica."""
        def get(self, *args, **kwargs):
            try:
                redis_replica = random.choice(redis_replicas)
                return redis_replica.get(*args, **kwargs)
            except redis.ConnectionError:
                return super(ReplicaProxyRedis, self).get(*args, **kwargs)

    if isinstance(settings.CACHEOPS_REDIS, six.string_types):
        redis_client = ReplicaProxyRedis.from_url(settings.CACHEOPS_REDIS)
    else:
        redis_client = ReplicaProxyRedis(**settings.CACHEOPS_REDIS)

### Lua script loader

import re
import os.path

STRIP_RE = re.compile(r'TOSTRIP.*/TOSTRIP', re.S)

@memoize
def load_script(name, strip=False):
    filename = os.path.join(os.path.dirname(__file__), 'lua/%s.lua' % name)
    with open(filename) as f:
        code = f.read()
    if strip:
        code = STRIP_RE.sub('', code)
    return redis_client.register_script(code)
