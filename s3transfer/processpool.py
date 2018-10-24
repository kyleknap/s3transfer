# Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
import ctypes
import logging
import math
import os
from collections import namedtuple
from multiprocessing import Process, Queue
from multiprocessing.managers import BaseManager
try:
    from multiprocessing import SimpleQueue
except ImportError:
    from multiprocessing.queues import SimpleQueue
import threading

from botocore.session import Session

from s3transfer.compat import rename_file
from s3transfer.futures import TransferFuture
from s3transfer.futures import TransferCoordinator
from s3transfer.download import S3_RETRYABLE_ERRORS
from s3transfer.download import RetriesExceededError
from s3transfer.utils import random_file_extension
from s3transfer.utils import calculate_range_parameter

logger = logging.getLogger(__name__)

MB = 1024 * 1024

DownloadFileRequest = namedtuple(
    'DownloadFileRequest',
    ['bucket', 'key', 'filename', 'extra_args',  'expected_size',
     'transfer_id']
)
GetObjectJob = namedtuple(
    'GetObjectJob',
    ['bucket', 'key', 'filename', 'extra_args', 'offset', 'final_filename',
     'transfer_id']
)
SHUTDOWN = 1


class ProcessTransferConfig(object):
    def __init__(self,
                 multipart_threshold=8 * MB,
                 multipart_chunksize=8 * MB,
                 max_request_processes=10):
        """Configuration for the ProcessPoolDownloader

        :param multipart_threshold: The threshold for which ranged downloads
            occur.

        :param multipart_chunksize: The chunk size of each ranged download

        :param max_request_processes: The maximum number of processes that
            will be making S3 API transfer-related requests at a time.
        """
        self.multipart_threshold = multipart_threshold
        self.multipart_chunksize = multipart_chunksize
        self.max_request_processes = max_request_processes


class ProcessPoolDownloader(object):
    def __init__(self, client_kwargs=None, config=None):
        """Downloads S3 objects using process pools

        :type client_kwargs: dict
        :param client_kwargs: The keyword arguments to provide when
            instantiating S3 clients. The arguments must match the keyword
            arguments provided to the
            `botocore.session.Session.create_client()` method.

        :type config: ProcessTransferConfig
        :param config: Configuration for the downloader
        """
        self._client_kwargs = client_kwargs
        if client_kwargs is None:
            self._client_kwargs = {}

        self._transfer_config = config
        if config is None:
            self._transfer_config = ProcessTransferConfig()

        self._manager = MyManager()
        self._manager.start()
        self._monitor = self._manager.TransferMonitor()
        self._submitter = Submitter(
            transfer_config=self._transfer_config,
            client_kwargs=self._client_kwargs,
            monitor=self._monitor
        )


    def download_file(self, bucket, key, filename, extra_args=None,
                      expected_size=None):
        """Downloads the object's contents to a file

        :type bucket: str
        :param bucket: The name of the bucket to download from

        :type key: str
        :param key: The name of the key to download from

        :type filename: str
        :param filename: The name of a file to download to.

        :type extra_args: dict
        :param extra_args: Extra arguments that may be passed to the
            client operation

        :type expected_size: int
        :param expected_size: The expected size in bytes of the download. If
            provided, the downloader will not call HeadObject to determine the
            object's size and use the provided value instead. The size is
            needed to determine whether to do a multipart download.

        :rtype: s3transfer.futures.TransferFuture
        :returns: Transfer future representing the download
        """
        transfer_id = 1
        #coordinator = self._xfer_manager.create_coordinator(transfer_id)
        #print('created coord', self._xfer_manager.get_coordinator(transfer_id))
        if extra_args is None:
            extra_args = {}
        self._submitter.submit(
            DownloadFileRequest(
                bucket=bucket, key=key, filename=filename,
                extra_args=extra_args, expected_size=expected_size,
                transfer_id=transfer_id
            )
        )
        return ProcessTransferFuture(self._monitor)
        #return TransferFuture(coordinator=coordinator)

    def start(self):
        self._submitter.start()

    def shutdown(self):
        """Shutdown the downloader

        It will wait till all downloads are complete before returning.
        """
        self._submitter.shutdown()
        #self._manager.shutdown()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *args):
        self.shutdown()


class ProcessTransferFuture(object):
    def __init__(self, monitor):
        self._monitor = monitor

    def result(self):
        pass

    def done(self):
        pass

    def __del__(self):
        print('delete')
        self._monitor.delete()


class MyManager(BaseManager):
    pass


class TransferMonitor(object):
    def __init__(self):
        self._transfers = {}
        self._lock = threading.Lock()

    def set_expected_jobs_to_complete(self, transfer_id, num_jobs):
        self._transfers[transfer_id] = num_jobs

    def announce_job_complete(self, transfer_id):
        with self._lock:
            self._transfers[transfer_id] -= 1
            return self._transfers[transfer_id]

    def delete(self):
        print('Called!')


MyManager.register('TransferMonitor', TransferMonitor)


class Submitter(Process):
    def __init__(self, transfer_config, client_kwargs, monitor):
        super(Submitter, self).__init__()
        self._transfer_config = transfer_config
        self._client_kwargs = client_kwargs
        self._request_queue = Queue(1000)
        self._worker_queue = SimpleQueue()
        self._max_workers = self._transfer_config.max_request_processes
        self._workers = []
        self._monitor = monitor
        self._client = None

    def run(self):
        self._client = Session().create_client('s3', **self._client_kwargs)
        self._start_workers()
        while True:
            request = self._request_queue.get()
            print(request)
            if request == SHUTDOWN:
                print('submitter shutdown')
                for _ in self._workers:
                    self._worker_queue.put(SHUTDOWN)
                for worker in self._workers:
                    worker.join()
                return
            self._submit_get_object_jobs(request)

    def submit(self, request):
        self._request_queue.put(request)

    def shutdown(self, timeout=None):
        print('called join')
        self._request_queue.put(SHUTDOWN)
        self.join(timeout)

    def _start_workers(self):
        for _ in range(self._max_workers):
            print('starting worker')
            worker = GetObjectWorker(
                self._worker_queue, self._client_kwargs, self._monitor)
            worker.start()
            self._workers.append(worker)

    def _submit_get_object_jobs(self, request):
        size = self._get_size(request)
        temp_filename = self._allocate_tempfile(request, size)
        job_generator = GetObjectJobGenerator(
            bucket=request.bucket, key=request.key,
            filename=temp_filename, transfer_config=self._transfer_config,
            extra_args=request.extra_args, client_kwargs=self._client_kwargs,
            size=size, final_filename=request.filename,
            monitor=self._monitor, transfer_id=request.transfer_id
        )
        for job in job_generator:
            self._worker_queue.put(job)

    def _get_size(self, request):
        expected_size = request.expected_size
        if expected_size is None:
            expected_size = self._client.head_object(
                Bucket=request.bucket, Key=request.key,
                **request.extra_args)['ContentLength']
        return expected_size

    def _allocate_tempfile(self, request, size):
        temp_filename = (
            request.filename + os.extsep + random_file_extension())
        with open(temp_filename, 'wb') as f:
            os.ftruncate(f.fileno(), size)
        return temp_filename


class GetObjectJobGenerator(object):
    def __init__(self, bucket, key, filename, transfer_config, extra_args=None,
                 client_kwargs=None, size=None, final_filename=None,
                 monitor=None, transfer_id=None):
        self._bucket = bucket
        self._key = key
        self._filename = filename
        self._extra_args = extra_args
        self._client_kwargs = client_kwargs
        self._transfer_config = transfer_config
        self._size = size
        self._final_filename = final_filename
        self._monitor = monitor
        self._transfer_id = transfer_id

    def __iter__(self):
        if self._size < self._transfer_config.multipart_threshold:
            self._monitor.set_expected_jobs_to_complete(self._transfer_id, 1)
            yield GetObjectJob(
                bucket=self._bucket,
                key=self._key,
                filename=self._filename,
                offset=0,
                extra_args=self._extra_args,
                final_filename=self._final_filename,
                transfer_id=self._transfer_id,
            )
        else:
            part_size = self._transfer_config.multipart_chunksize
            num_parts = int(math.ceil(self._size / float(part_size)))
            self._monitor.set_expected_jobs_to_complete(
                self._transfer_id, num_parts)
            for i in range(num_parts):
                offset = i * self._transfer_config.multipart_chunksize
                # Calculate the range parameter
                range_parameter = calculate_range_parameter(
                    part_size, i, num_parts)
                get_object_kwargs = {'Range': range_parameter}
                get_object_kwargs.update(self._extra_args)
                yield GetObjectJob(
                    bucket=self._bucket,
                    key=self._key,
                    filename=self._filename,
                    offset=offset,
                    extra_args=get_object_kwargs,
                    final_filename=self._final_filename,
                    transfer_id=self._transfer_id,
                )
        raise StopIteration()


class GetObjectWorker(Process):
    _MAX_ATTEMPTS = 5
    _IO_CHUNKSIZE = 2 * MB

    def __init__(self, queue, client_kwargs, monitor):
        super(GetObjectWorker, self).__init__()
        self._queue = queue
        self._client_kwargs = client_kwargs
        self._monitor = monitor
        self._client = None

    def run(self):
        self._client = Session().create_client('s3', **self._client_kwargs)
        while True:
            job = self._queue.get()
            if job == SHUTDOWN:
                return
            try:
                #print('got get object job')
                self._do_get_object(
                    bucket=job.bucket, key=job.key, filename=job.filename,
                    extra_args=job.extra_args, offset=job.offset
                )
            except Exception as e:
                print(e)
            finally:
                remaining = self._monitor.announce_job_complete(
                    job.transfer_id)
                #print(remaining)
                if not remaining:
                    rename_file(job.filename, job.final_filename)

    def _do_get_object(self, bucket, key, extra_args, filename, offset):
        for i in range(self._MAX_ATTEMPTS):
            try:
                response = self._client.get_object(
                    Bucket=bucket, Key=key, **extra_args)
                self._write_to_file(filename, offset, response)
                return
            except S3_RETRYABLE_ERRORS as e:
                logger.debug("Retrying exception caught (%s), "
                             "retrying request, (attempt %s / %s)", e, i,
                             self._MAX_ATTEMPTS, exc_info=True)
                last_exception = e
            raise RetriesExceededError(last_exception)

    def _write_to_file(self, filename, offset, response):
        with open(filename, 'rb+') as f:
            f.seek(offset)
            chunks = iter(
                lambda: response['Body'].read(self._IO_CHUNKSIZE), b'')
            for chunk in chunks:
                f.write(chunk)
