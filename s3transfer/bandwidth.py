# Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You
# may not use this file except in compliance with the License. A copy of
# the License is located at
#
# http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied. See the License for the specific
# language governing permissions and limitations under the License.
import time
import threading


class RequestExceededException(Exception):
    def __init__(self, requested_amt, retry_time):
        """Error

        :type requested_amt: int
        :param requested_amt: The originally requested byte amount

        :type retry_time: float
        :param retry_time: The length in time to wait to retry for the
            requested amount
        """
        self.requested_amt = requested_amt
        self.retry_time = retry_time
        msg = (
            'Request amount %s exceeded the amount available. Retry in %s' % (
                requested_amt, retry_time)
        )
        super(RequestExceededException, self).__init__(msg)


class TimeUtils(object):
    def time(self):
        """Get the current time back

        :rtype: float
        :returns: The current time in seconds
        """
        return time.time()

    def sleep(self, value):
        """Sleep for a designated time

        :type value: float
        :param value: The time to sleep for in seconds
        """
        return time.sleep(value)


class BandwidthLimiter(object):
    def __init__(self, token_bucket, time_utils=None):
        """Limits bandwidth for shared S3 transfers

        :type token_bucket: TokenBucket
        :param token_bucket: The token bucket to use limit bandwidth

        :type time_utils: TimeUtils
        :param time_utils: Time utility to use for interacting with time.
        """
        self._token_bucket = token_bucket
        self._time_utils = time_utils
        if not time_utils:
            self._time_utils = TimeUtils()

    def get_bandwith_limited_stream(self, fileobj, transfer_coordinator,
                                    enabled=True):
        stream = BandwidthLimitedStream(
            fileobj, self._token_bucket, transfer_coordinator,
            self._time_utils)
        if not enabled:
            stream.disable()
        return stream


class BandwidthLimitedStream(object):
    def __init__(self, fileobj, token_bucket, transfer_coordinator,
                 time_utils=None):
        """Limits bandwidth for reads on a wrapped stream

        :type fileobj: file-like object
        :param fileobj: The file like object to wrap

        :type token_bucket: TokenBucket
        :param token_bucket: The token bucket to use to throttle reads on
            the stream

        :type transfer_coordinator: s3transfer.futures.TransferCoordinator
        param transfer_coordinator: The coordinator for the general transfer
            that the wrapped stream is a part of

        :type time_utils: TimeUtils
        :param time_utils: The time utility to use for interacting with time
        """
        self._fileobj = fileobj
        self._token_bucket = token_bucket
        self._transfer_coordinator = transfer_coordinator
        self._time_utils = time_utils
        if not time_utils:
            self._time_utils = TimeUtils()
        self._enabled = True

    def enable(self):
        """Enable bandwidth limiting on reads to the stream"""
        self._enabled = True

    def disable(self):
        """Disable bandwidth limiting on reads to the stream"""
        self._enabled = False

    def read(self, amount):
        """Read a specified amount

        Reads will only be throttled if bandwidth limiting is enabled.
        """
        if not self._enabled:
            return self._fileobj.read(amount)

        while not self._transfer_coordinator.exception:
            try:
                self._token_bucket.consume(amount)
                return self._fileobj.read(amount)
            except RequestExceededException as e:
                self._time_utils.sleep(e.retry_time)
        else:
            raise self._transfer_coordinator.exception

    def seek(self, where):
        self._fileobj.seek(where)

    def tell(self):
        return self._fileobj.tell()

    def close(self):
        self._fileobj.close()

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        self.close()


class TokenBucket(object):
    def __init__(self, refill_rate, time_utils=None):
        """Limits bandwidths using a token bucket algorithm

        :type refill_rate: int
        :param refill_rate: The rate in which to refill tokens. The rate
            is in terms of tokens per second.

        :type time_utils: TimeUtils()
        :param time_utils: The time utility to use for interacting with time.
        """
        self._refill_rate = refill_rate
        self._tokens = 0
        self._time_utils = time_utils
        if time_utils is None:
            self._time_utils = TimeUtils()
        self._lock = threading.Lock()
        self._last_time = None

    def consume(self, amt):
        """Consume a specified amt of tokens

        :type amt: int
        :param amt: The number of tokens requesting to consume

        :raises RequestExceededException: If the amount of tokens requested
            exceeds the number of tokens available.

        :rtype: int
        :returns: The number of tokens allowed to consume
        """
        with self._lock:
            if self._last_time is None:
                self._last_time = self._time_utils.time()

            time_now = self._time_utils.time()
            elapsed_time = time_now - self._last_time
            self._last_time = time_now

            self._tokens += int(self._refill_rate * elapsed_time)
            if amt > self._tokens:
                retry_time = (amt - self._tokens)/float(self._refill_rate)
                raise RequestExceededException(
                    requested_amt=amt, retry_time=retry_time)

            self._tokens = self._tokens - amt
            return amt
