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
import collections
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
        self._request_token = object()

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
                self._token_bucket.consume(amount, self._request_token)
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


class TokenStatTracker(object):
    def __init__(self):
        self.token_consumption_stats = []

    def add_consumption_stat(self, amt_requested, is_success, request_time):
        self.token_consumption_stats.append(
            {
                'amt_requested': amt_requested,
                'is_success': is_success,
                'request_time': request_time,
                'thread_id': threading.current_thread().ident
            }
        )

class ConsumptionRateTracker(object):
    def __init__(self, time_utils=None):
        if time_utils is None:
            self._time_utils = TimeUtils()
        self._last_consume_time = None
        self._current_rates = []
        self._max_length = 5

    @property
    def current_rate(self):
        num_collected_rates = len(self._current_rates)
        if not num_collected_rates:
            return 0
        return sum(self._current_rates)/float(num_collected_rates)

    def get_projected_rate(self, amt, time_at_consumption):
        individual_rate = self._get_rate_for_individual_point(
            amt, time_at_consumption)
        num_collected_rates = len(self._current_rates)
        total_rates = num_collected_rates * self.current_rate + individual_rate
        return total_rates/(num_collected_rates + 1)

    def record_consumption_rate(self, amt, time_at_consumption):
        rate = self._get_rate_for_individual_point(amt, time_at_consumption)
        if self._last_consume_time is None:
            self._last_consume_time = time_at_consumption
        self._update_current_rates(rate)

    def _get_rate_for_individual_point(self, amt, time_at_consumption):
        if self._last_consume_time is None:
            return 0
        return amt/(time_at_consumption - self._last_consume_time)

    def _update_current_rates(self, rate):
        self._current_rates.append(rate)
        if len(self._current_rates) > self._max_length:
            self._current_rates.pop(0)


class LeakyBucket(object):
    def __init__(self, rate, time_utils=None, stats=None):
        self._max_rate = rate
        self._time_utils = time_utils
        if time_utils is None:
            self._time_utils = TimeUtils()
        self._lock = threading.Lock()
        self._rate_tracker = ConsumptionRateTracker(self._time_utils)
        self._last_consume_time = None
        self.stats = stats
        self._retry_queue = collections.OrderedDict()
        self._total_wait = 0

    def consume(self, amt, request_token):
        with self._lock:
            time_now = self._time_utils.time()
            projected_rate = self._rate_tracker.get_projected_rate(
                amt, time_now)
            #print(projected_rate)
            #proposed_rate = time_now - self._last_consume_time
            #max_allowed_amt = self._rate * elapsed_time

            if self._retry_queue:
                if request_token not in self._retry_queue:
                    allocated_time = amt/self._max_rate
                    self._total_wait += allocated_time
                    self._add_to_retry_queue(
                        request_token, allocated_time, self._total_wait,
                        time_now)
                    self._raise_retry(amt, self._total_wait, time_now)

                elif request_token is not list(self._retry_queue)[0]:
                    self._retry_for_premature_consume_request(
                        amt, request_token, time_now)

                else:
                    if projected_rate > self._max_rate:
                        self._retry_for_premature_consume_request(
                            amt, request_token, time_now)
                    else:
                        scheduled_retry = self._retry_queue.pop(
                            request_token)
                        self._total_wait -= scheduled_retry['slot_time']
                        return self._release_tokens(amt, time_now)

            elif projected_rate > self._max_rate:
                print('here')
                retry_time = amt/(projected_rate - self._max_rate)
                self._total_wait += retry_time
                self._add_to_retry_queue(
                    request_token, retry_time, retry_time, time_now)
                self._raise_retry(amt, retry_time, time_now)

            else:
                return self._release_tokens(amt, time_now)

    def _add_to_retry_queue(self, request_token, allocated_time, wait_duration,
                            time_now):
        self._retry_queue[request_token] = {
            'wait_duration': wait_duration,
            'slot_time': allocated_time,
            'scheduled_time': time_now + wait_duration,
        }

    def _retry_for_premature_consume_request(self, amt, request_token,
                                             time_now):
        scheduled_retry = self._retry_queue[request_token]
        time_to_wait = max(
            scheduled_retry['scheduled_time'] - time_now, 0)
        self._raise_retry(amt, time_to_wait, time_now)

    def _raise_retry(self, amt, retry_time, time_now):
        if self.stats:
            self._add_stats(amt, False, time_now)
        if retry_time < 0:
            print(retry_time, self._total_wait, self._retry_queue)
        raise RequestExceededException(
            requested_amt=amt, retry_time=retry_time)

    def _release_tokens(self, amt, time_now):
        if self.stats:
            self._add_stats(amt, True, time_now)
        self._rate_tracker.record_consumption_rate(amt, time_now)
        return amt

    def _add_stats(self, amt_requested, is_success, request_time):
        self.stats.add_consumption_stat(
            amt_requested, is_success, request_time)


class LeakyBucketV0(object):
    def __init__(self, rate, time_utils=None, stats=None):
        self._rate = rate
        self._time_utils = time_utils
        if time_utils is None:
            self._time_utils = TimeUtils()
        self._lock = threading.Lock()
        self._last_consume_time = None
        self.stats = stats
        self._request_tokens = collections.defaultdict(int)

    def consume(self, amt, request_token=None):
        with self._lock:
            if self._last_consume_time is None:
                self._last_consume_time = self._time_utils.time()
                return amt

            time_now = self._time_utils.time()
            elapsed_time = time_now - self._last_consume_time
            max_allowed_amt = self._rate * elapsed_time

            if amt > max_allowed_amt:
                if self.stats:
                    self._add_stats(amt, False, time_now)

                retry_time = (amt - max_allowed_amt)/self._rate
                previous_time_waiting = self._request_tokens[request_token]
                if previous_time_waiting > 5 * amt/float(self._rate):
                    retry_time = 0.1 * retry_time
                self._request_tokens[request_token] += retry_time
                raise RequestExceededException(
                    requested_amt=amt, retry_time=retry_time)

            if self.stats:
                self._add_stats(amt, True, time_now)
            self._last_consume_time = self._time_utils.time()
            self._request_tokens.pop(request_token, None)
            return amt

    def _add_stats(self, amt_requested, is_success, request_time):
        self.stats.add_consumption_stat(
            amt_requested, is_success, request_time)


class TokenBucket(object):
    def __init__(self, refill_rate, time_utils=None, stats=None):
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
        self.stats = stats

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
                if self.stats:
                    self._add_stats(amt, False, time_now)
                retry_time = (amt - self._tokens)/float(self._refill_rate)
                raise RequestExceededException(
                    requested_amt=amt, retry_time=retry_time)

            if self.stats:
                self._add_stats(amt, True, time_now)
            self._tokens = self._tokens - amt
            return amt

    def _add_stats(self, amt_requested, is_success, request_time):
        self.stats.add_consumption_stat(
            amt_requested, is_success, request_time)
