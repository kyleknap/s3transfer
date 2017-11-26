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


class LeakyBucket(object):
    def __init__(self, rate, time_utils=None, stats=None):
        self._max_rate = float(rate)
        self._time_utils = time_utils
        if time_utils is None:
            self._time_utils = TimeUtils()
        self._lock = threading.Lock()
        self._rate_tracker = ConsumptionRateTracker(self._time_utils)
        self._consumption_scheduler = ConsumptionScheduler()
        self.stats = stats

    def consume(self, amt, request_token):
        with self._lock:
            time_now = self._time_utils.time()
            if self._consumption_scheduler.is_scheduled(request_token):
                self._consume_for_scheduled_consumption(
                    amt, request_token, time_now)
            elif self._at_near_max_bandwidth_capacity():
                return self._schedule_during_max_bandwidth_usage(
                    amt, request_token, time_now)
            elif self._projected_to_exceed_max_bandwidth(amt, time_now):
                self._schedule_using_projected_rate(
                    amt, request_token, time_now)
            else:
                return self._release_tokens(amt, time_now)

    def _projected_to_exceed_max_bandwidth(self, amt, time_now):
        projected_rate = self._rate_tracker.get_projected_rate(amt, time_now)
        return projected_rate > self._max_rate

    def _at_near_max_bandwidth_capacity(self):
        return (self._rate_tracker.current_rate / self._max_rate) > 0.8

    def _consume_for_scheduled_consumption(self, amt, request_token, time_now):
        self._consumption_scheduler.process_scheduled_consumption(
            request_token)
        return self._release_tokens(amt, time_now)

    def _schedule_during_max_bandwidth_usage(self, amt, request_token,
                                             time_now):
        allocated_time = amt/float(self._max_rate)
        wait_time = self._consumption_scheduler.schedule_consumption(
            amt, request_token, allocated_time)
        self._raise_retry(amt, wait_time, time_now)

    def _schedule_using_projected_rate(self, amt, request_token, time_now):
        projected_rate = self._rate_tracker.get_projected_rate(
            amt, time_now)
        allocated_time = amt/(projected_rate - self._max_rate)
        wait_time = self._consumption_scheduler.schedule_consumption(
            amt, request_token, allocated_time)
        self._raise_retry(amt, wait_time, time_now)

    def _raise_retry(self, amt, retry_time, time_now):
        if self.stats:
            self._add_stats(amt, False, time_now)
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


class ConsumptionScheduler(object):
    def __init__(self):
        self._tokens_to_scheduled_consumption = {}
        self._total_wait = 0

    def is_scheduled(self, token):
        return token in self._tokens_to_scheduled_consumption

    def schedule_consumption(self, amt, token, time_to_consume):
        self._total_wait += time_to_consume
        self._tokens_to_scheduled_consumption[token] = {
            'wait_duration': self._total_wait,
            'time_to_consume': time_to_consume,
        }
        return self._total_wait

    def process_scheduled_consumption(self, token):
        scheduled_retry = self._tokens_to_scheduled_consumption.pop(token)
        self._total_wait = max(
            self._total_wait - scheduled_retry['time_to_consume'], 0)


class ConsumptionRateTracker(object):
    def __init__(self, time_utils=None):
        if time_utils is None:
            self._time_utils = TimeUtils()
        self._tracked_consumptions = []
        self._total_consumed = 0
        self._max_length = 10

    @property
    def current_rate(self):
        if len(self._tracked_consumptions) <= 1:
            return 0
        time_delta = float(self._latest_time_point - self._oldest_time_point)
        return self._total_consumed/time_delta

    @property
    def _oldest_time_point(self):
        return self._tracked_consumptions[0][1]

    @property
    def _latest_time_point(self):
        return self._tracked_consumptions[-1][1]

    def get_projected_rate(self, amt, time_at_consumption):
        if not self._tracked_consumptions:
            return 0
        time_delta = time_at_consumption - self._oldest_time_point
        return (self._total_consumed + amt)/time_delta

    def record_consumption_rate(self, amt, time_at_consumption):
        self._tracked_consumptions.append((amt, time_at_consumption))
        self._total_consumed += amt
        if len(self._tracked_consumptions) > self._max_length:
            old_amt, _ = self._tracked_consumptions.pop(0)
            self._total_consumed -= old_amt
