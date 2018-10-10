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
import logging
import math
import os
import time
from multiprocessing import Process

from botocore.session import Session

from s3transfer.compat import rename_file
from s3transfer.compat import filter
from s3transfer.download import S3_RETRYABLE_ERRORS
from s3transfer.download import RetriesExceededError
from s3transfer.download import DownloadChunkIterator
from s3transfer.utils import random_file_extension
from s3transfer.utils import calculate_range_parameter

logger = logging.getLogger(__name__)

MB = 1024 * 1024


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

        self._config = config
        if config is None:
            self._config = ProcessTransferConfig()

        self._spawner = ProcessTaskSpawner(
            max_processes=self._config.max_request_processes
        )

        self._session = Session()
        self._client = self._session.create_client('s3', **self._client_kwargs)

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
        if extra_args is None:
            extra_args = {}
        if expected_size is None:
            expected_size = self._client.head_object(
                Bucket=bucket, Key=key, **extra_args)['ContentLength']
        self._do_download_file(
            bucket=bucket, key=key, filename=filename,
            extra_args=extra_args, size=expected_size
        )

    def _do_download_file(self, bucket, key, filename, extra_args, size):
        temp_filename = filename + os.extsep + random_file_extension()
        if size < self._config.multipart_threshold:
            self._do_non_ranged_download(
                bucket=bucket, key=key, filename=temp_filename,
                extra_args=extra_args)
        else:
            self._do_ranged_download(
                bucket=bucket, key=key, filename=temp_filename,
                extra_args=extra_args, size=size)

        rename_file(temp_filename, filename)

    def _do_non_ranged_download(self, bucket, key, filename, extra_args):
        process = self._spawner.spawn(
            get_object, bucket=bucket, key=key,
            filename=filename, extra_args=extra_args,
            client_kwargs=self._client_kwargs
        )
        self._wait_for_process(process)

    def _do_ranged_download(self, bucket, key, filename, extra_args, size):
        part_size = self._config.multipart_chunksize
        num_parts = int(math.ceil(size / float(part_size)))
        processes = []
        for i in range(num_parts):
            offset = i * self._config.multipart_chunksize
            # Calculate the range parameter
            range_parameter = calculate_range_parameter(
                part_size, i, num_parts)
            get_object_kwargs = {'Range': range_parameter}
            get_object_kwargs.update(extra_args)
            p = self._spawner.spawn(
                get_object, bucket=bucket, key=key,
                filename=filename, extra_args=get_object_kwargs, offset=offset,
                client_kwargs=self._client_kwargs
            )
            processes.append(p)
        for p in processes:
            self._wait_for_process(p)

    def _wait_for_process(self, process):
        process.join()
        if process.exitcode != 0:
            raise RuntimeError(
                'Process %s had rc of %s' % (process.pid, process.exitcode))

    def shutdown(self):
        """Shutdown the downloader

        It will wait till all downloads are complete before returning.
        """
        pass

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.shutdown()


class ProcessTaskSpawner(object):
    def __init__(self, max_processes):
        self._max_processes = max_processes
        self._process_count = 0
        self._processes = []

    def spawn(self, func, *args, **kwargs):
        self._clear_completed_process()
        while self._process_count == self._max_processes:
            time.sleep(0.1)
            self._clear_completed_process()
        return self._spawn_process(func, args, kwargs)

    def _clear_completed_process(self):
        self._processes = list(
            filter(lambda p: p.exitcode is None, self._processes))
        self._process_count = len(self._processes)

    def _spawn_process(self, func, args, kwargs):
        process = Process(target=func, args=args, kwargs=kwargs)
        process.start()
        self._process_count += 1
        self._processes.append(process)
        return process


def get_object(bucket, key, filename, extra_args=None, offset=0,
               max_attempts=5, client_kwargs=None):
    session = Session()
    if extra_args is None:
        extra_args = {}

    if client_kwargs is None:
        client_kwargs = {}

    client = session.create_client('s3', **client_kwargs)
    with open(filename, 'wb') as f:
        for i in range(max_attempts):
            try:
                _do_get_object(
                    client=client, bucket=bucket, key=key, fileobj=f,
                    offset=offset, extra_args=extra_args
                )
                return
            except S3_RETRYABLE_ERRORS as e:
                logger.debug("Retrying exception caught (%s), "
                             "retrying request, (attempt %s / %s)", e, i,
                             max_attempts, exc_info=True)
                last_exception = e

            raise RetriesExceededError(last_exception)


def _do_get_object(client, bucket, key, fileobj, offset, extra_args):
    response = client.get_object(Bucket=bucket, Key=key, **extra_args)
    chunks = DownloadChunkIterator(response['Body'], 1024)
    fileobj.seek(offset)
    for chunk in chunks:
        fileobj.write(chunk)
