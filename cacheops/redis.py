from __future__ import absolute_import
import warnings
from contextlib import contextmanager
import six
import sys
import traceback

from funcy import decorator, identity, memoize, LazyObject
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

client_class = redis.StrictRedis
custom_client_class = getattr(settings, 'CACHEOPS_CLIENT_CLASS', None)
if custom_client_class:
    client_class = import_string(custom_client_class)


LOCK_TIMEOUT = 60


class CacheopsRedis(client_class):
    get = handle_connection_failure(redis.StrictRedis.get)

    """ Handles failover of AWS elasticache
    """
    def execute_command(self, *args, **options):
        try:
            return super(CacheopsRedis, self).execute_command(*args, **options)
        except redis.ResponseError as e:
            if "READONLY" not in e.message:
                raise
            connection = self.connection_pool.get_connection(args[0], **options)
            connection.disconnect()
            warnings.warn("Primary probably failed over, reconnecting")
            return super(CacheopsRedis, self).execute_command(*args, **options)

    @contextmanager
    def getting(self, key, lock=False):
        if not lock:
            yield self.get(key)
        else:
            locked = False
            try:
                data = self._get_or_lock(key)
                locked = data is None
                yield data
            finally:
                if locked:
                    self._release_lock(key)

    @handle_connection_failure
    def _get_or_lock(self, key):
        self._lock = getattr(self, '_lock', self.register_script("""
            local locked = redis.call('set', KEYS[1], 'LOCK', 'nx', 'ex', ARGV[1])
            if locked then
                redis.call('del', KEYS[2])
            end
            return locked
        """))
        signal_key = key + ':signal'

        while True:
            data = self.get(key)
            if data is None:
                if self._lock(keys=[key, signal_key], args=[LOCK_TIMEOUT]):
                    return None
            elif data != b'LOCK':
                return data

            # No data and not locked, wait
            self.brpoplpush(signal_key, signal_key, timeout=LOCK_TIMEOUT)

    @handle_connection_failure
    def _release_lock(self, key):
        self._unlock = getattr(self, '_unlock', self.register_script("""
            if redis.call('get', KEYS[1]) == 'LOCK' then
                redis.call('del', KEYS[1])
            end
            redis.call('lpush', KEYS[2], 1)
            redis.call('expire', KEYS[2], 1)
        """))
        signal_key = key + ':signal'
        self._unlock(keys=[key, signal_key])


CacheopsRedis = CacheopsRedis if settings.CACHEOPS_DEGRADE_ON_FAILURE else client_class
try:
    # the conf could be a list of string
    # list would look like: ["redis://cache-001:6379/1", "redis://cache-002:6379/2"]
    # string would be: "redis://cache-001:6379/1,redis://cache-002:6379/2"
    redis_replica_conf = settings.CACHEOPS_REDIS_REPLICA
    if isinstance(redis_replica_conf, six.string_types):
        redis_replicas = map(redis.StrictRedis.from_url, redis_replica_conf.split(','))
    else:
        redis_replicas = map(redis.StrictRedis.from_url, redis_replica_conf)
except AttributeError as err:
    @LazyObject
    def redis_client():
        # Allow client connection settings to be specified by a URL.
        if isinstance(settings.CACHEOPS_REDIS, six.string_types):
            return CacheopsRedis.from_url(settings.CACHEOPS_REDIS)
        else:
            return CacheopsRedis(**settings.CACHEOPS_REDIS)

else:

    class ReplicaProxyRedis(CacheopsRedis):
        """ Proxy `get` calls to redis replica.
        """

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
