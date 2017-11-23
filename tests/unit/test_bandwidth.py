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
import os
import random
import shutil
import tempfile
import threading

import mock

from tests import unittest
from s3transfer.bandwidth import RequestExceededException
from s3transfer.bandwidth import TimeUtils
from s3transfer.bandwidth import BandwidthLimiter
from s3transfer.bandwidth import BandwidthLimitedStream
from s3transfer.bandwidth import TokenBucket
from s3transfer.futures import TransferCoordinator


class SingleIncrementsTimeUtils(TimeUtils):
    def __init__(self):
        self._count = 0

    def time(self):
        current_count = self._count
        self._count += 1
        return current_count


class TestTimeUtils(unittest.TestCase):
    @mock.patch('time.time')
    def test_time(self, mock_time):
        mock_return_val = 1
        mock_time.return_value = mock_return_val
        time_utils = TimeUtils()
        self.assertEqual(time_utils.time(), mock_return_val)

    @mock.patch('time.sleep')
    def test_sleep(self, mock_sleep):
        time_utils = TimeUtils()
        time_utils.sleep(1)
        self.assertEqual(
            mock_sleep.call_args_list,
            [mock.call(1)]
        )


class TestBandwidthLimiter(unittest.TestCase):
    def setUp(self):
        self.token_bucket = mock.Mock(TokenBucket)
        self.bandwidth_limiter = BandwidthLimiter(self.token_bucket)
        self.tempdir = tempfile.mkdtemp()
        self.content = b'my content'
        self.filename = os.path.join(self.tempdir, 'myfile')
        with open(self.filename, 'wb') as f:
            f.write(self.content)
        self.coordinator = TransferCoordinator()

    def tearDown(self):
        shutil.rmtree(self.tempdir)

    def test_get_bandwidth_limited_stream(self):
        with open(self.filename, 'rb') as f:
            stream = self.bandwidth_limiter.get_bandwith_limited_stream(
                f, self.coordinator)
            self.assertIsInstance(stream, BandwidthLimitedStream)
            self.assertEqual(stream.read(len(self.content)), self.content)
            self.assertEqual(
                self.token_bucket.consume.call_args_list,
                [mock.call(len(self.content))]
            )

    def test_get_disabled_bandwidth_limited_stream(self):
        with open(self.filename, 'rb') as f:
            stream = self.bandwidth_limiter.get_bandwith_limited_stream(
                f, self.coordinator, enabled=False)
            self.assertIsInstance(stream, BandwidthLimitedStream)
            self.assertEqual(stream.read(len(self.content)), self.content)
            self.token_bucket.consume.assert_not_called()


class TestBandwidthLimitedStream(unittest.TestCase):
    def setUp(self):
        self.token_bucket = mock.Mock(TokenBucket)
        self.time_utils = mock.Mock(TimeUtils)
        self.tempdir = tempfile.mkdtemp()
        self.content = b'my content'
        self.filename = os.path.join(self.tempdir, 'myfile')
        with open(self.filename, 'wb') as f:
            f.write(self.content)
        self.coordinator = TransferCoordinator()

    def tearDown(self):
        shutil.rmtree(self.tempdir)

    def test_read(self):
        with open(self.filename, 'rb') as f:
            stream = BandwidthLimitedStream(
                f, self.token_bucket, self.coordinator, self.time_utils)
            data = stream.read(len(self.content))
            self.assertEqual(self.content, data)
            self.assertEqual(
                self.token_bucket.consume.call_args_list,
                [mock.call(len(self.content))]
            )
            self.assertFalse(self.time_utils.sleep.called)

    def test_retries_on_request_exceeded(self):
        with open(self.filename, 'rb') as f:
            stream = BandwidthLimitedStream(
                f, self.token_bucket, self.coordinator, self.time_utils)
            retry_time = 1
            amt_requested = len(self.content)
            self.token_bucket.consume.side_effect = [
                RequestExceededException(amt_requested, retry_time),
                len(self.content)
            ]
            data = stream.read(len(self.content))
            self.assertEqual(self.content, data)
            self.assertEqual(
                self.token_bucket.consume.call_args_list,
                [mock.call(amt_requested), mock.call(amt_requested)]
            )
            self.assertEqual(
                self.time_utils.sleep.call_args_list,
                [mock.call(retry_time)]
            )

    def test_with_transfer_coordinator_exception(self):
        self.coordinator.set_exception(ValueError())
        with open(self.filename, 'rb') as f:
            stream = BandwidthLimitedStream(
                f, self.token_bucket, self.coordinator, self.time_utils)
            with self.assertRaises(ValueError):
                stream.read(len(self.content))

    def test_read_when_disabled(self):
        with open(self.filename, 'rb') as f:
            stream = BandwidthLimitedStream(
                f, self.token_bucket, self.coordinator, self.time_utils)
            stream.disable()
            data = stream.read(len(self.content))
            self.assertEqual(self.content, data)
            self.assertFalse(self.token_bucket.consume.called)

    def test_read_toggle_disable_enable(self):
        with open(self.filename, 'rb') as f:
            stream = BandwidthLimitedStream(
                f, self.token_bucket, self.coordinator, self.time_utils)
            stream.disable()
            data = stream.read(1)
            self.assertEqual(self.content[:1], data)
            self.assertFalse(self.token_bucket.consume.called)
            stream.enable()
            data = stream.read(len(self.content) - 1)
            self.assertEqual(self.content[1:], data)
            self.assertEqual(
                self.token_bucket.consume.call_args_list,
                [mock.call(len(self.content) - 1)]
            )

    def test_seek(self):
        mock_fileobj = mock.Mock()
        stream = BandwidthLimitedStream(
            mock_fileobj, self.token_bucket, self.coordinator,
            self.time_utils)
        stream.seek(1)
        self.assertEqual(
            mock_fileobj.seek.call_args_list,
            [mock.call(1)]
        )

    def test_tell(self):
        mock_fileobj = mock.Mock()
        stream = BandwidthLimitedStream(
            mock_fileobj, self.token_bucket, self.coordinator,
            self.time_utils)
        stream.tell()
        self.assertEqual(
            mock_fileobj.tell.call_args_list,
            [mock.call()]
        )

    def test_close(self):
        mock_fileobj = mock.Mock()
        stream = BandwidthLimitedStream(
            mock_fileobj, self.token_bucket, self.coordinator,
            self.time_utils)
        stream.close()
        self.assertEqual(
            mock_fileobj.close.call_args_list,
            [mock.call()]
        )

    def test_context_manager(self):
        mock_fileobj = mock.Mock()
        stream = BandwidthLimitedStream(
            mock_fileobj, self.token_bucket, self.coordinator,
            self.time_utils)
        with stream as stream_handle:
            self.assertIs(stream_handle, stream)
        self.assertEqual(
            mock_fileobj.close.call_args_list,
            [mock.call()]
        )


class TestTokenBucket(unittest.TestCase):
    def setUp(self):
        self.time_utils = SingleIncrementsTimeUtils()

    def test_consume(self):
        refill_rate = 1
        # Tokens will be regenerated with one token on each
        # consume()
        token_bucket = TokenBucket(refill_rate, self.time_utils)
        self.assertEqual(token_bucket.consume(1), 1)

        # Another consume requesting one token should be allowed as
        # the bucket fills back up by one for the next access.
        self.assertEqual(token_bucket.consume(1), 1)

        with self.assertRaises(RequestExceededException):
            # The refill rate will only allow for only a single token
            # to be added leaving only one token available.
            token_bucket.consume(2)

    def test_calculates_retry_time(self):
        refill_rate = 1
        # Tokens will be regenerated with one token on each
        # consume()
        token_bucket = TokenBucket(refill_rate, self.time_utils)
        try:
            token_bucket.consume(2)
            self.fail('A RequestExceededException should have been thrown')
        except RequestExceededException as e:
            self.assertEqual(e.requested_amt, 2)
            # There will be enough tokens on the next access as tokens
            # are generate at each time tick.
            self.assertEqual(e.retry_time, 1.0)


class TestTokenBucketThreadingProperties(unittest.TestCase):
    def setUp(self):
        self.threads = []
        self.collected_tokens = []

    def tearDown(self):
        self.join_threads()

    def join_threads(self):
        for thread in self.threads:
            thread.join()
        self.threads = []

    def start_threads(self):
        for thread in self.threads:
            thread.start()

    def test_does_not_consume_more_than_requested(self):
        time_increments = 50
        num_threads = time_increments
        refill_rate = 1
        num_per_consume = 1

        time_utils = SingleIncrementsTimeUtils()
        token_bucket = TokenBucket(refill_rate, time_utils)

        def consume():
            try:
                self.collected_tokens.append(
                    token_bucket.consume(num_per_consume))
            except RequestExceededException:
                pass

        for _ in range(num_threads):
            t = threading.Thread(target=consume)
            self.threads.append(t)
        self.start_threads()
        self.join_threads()
        self.assertEqual(
            sum(self.collected_tokens), num_threads * num_per_consume)

    def test_does_not_exceed_theoretical_maximum(self):
        time_increments = 50
        num_threads = time_increments
        refill_rate = 1
        min_request = 1
        max_request = 5

        time_utils = SingleIncrementsTimeUtils()
        token_bucket = TokenBucket(refill_rate, time_utils)

        def consume_random():
            num_requested = random.randint(min_request, max_request)
            try:
                self.collected_tokens.append(
                    token_bucket.consume(num_requested))
            except RequestExceededException:
                pass

        for _ in range(num_threads):
            t = threading.Thread(target=consume_random)
            self.threads.append(t)
        self.start_threads()
        self.join_threads()
        self.assertLessEqual(
            sum(self.collected_tokens), time_increments * refill_rate)

    def test_returns_retry_times_in_theoretical_range(self):
        time_increments = 50
        num_threads = time_increments
        refill_rate = 1
        min_request = 1
        max_request = 5

        time_utils = SingleIncrementsTimeUtils()
        token_bucket = TokenBucket(refill_rate, time_utils)
        request_exceeded_exceptions = []

        def consume_and_capture_request_exceeds():
            num_requested = random.randint(min_request, max_request)
            try:
                token_bucket.consume(num_requested)
            except RequestExceededException as e:
                request_exceeded_exceptions.append(e)

        for _ in range(num_threads):
            t = threading.Thread(target=consume_and_capture_request_exceeds)
            self.threads.append(t)
        self.start_threads()
        self.join_threads()
        for exception in request_exceeded_exceptions:
            # Given the bucket refills by one with each access at best
            # it will only have to wait one tick of the time
            self.assertGreaterEqual(exception.retry_time, refill_rate)
            # Then at worst it will be the amount requested as the bucket
            # could be completely empty and would have to wait that many
            # ticks for the bucket to replenish
            self.assertLessEqual(
                exception.retry_time, exception.requested_amt)
