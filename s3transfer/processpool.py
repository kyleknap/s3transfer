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
import collections
import logging
import multiprocessing
import multiprocessing.managers
import threading

import botocore.session

from s3transfer.constants import MB
from s3transfer.constants import ALLOWED_DOWNLOAD_ARGS
from s3transfer.compat import MAXINT
from s3transfer.exceptions import CancelledError
from s3transfer.exceptions import RetriesExceededError
from s3transfer.futures import BaseTransferFuture
from s3transfer.futures import BaseTransferMeta
from s3transfer.utils import S3_RETRYABLE_DOWNLOAD_ERRORS
from s3transfer.utils import calculate_num_parts
from s3transfer.utils import calculate_range_parameter
from s3transfer.utils import OSUtils
from s3transfer.utils import CallArgs

logger = logging.getLogger(__name__)

SHUTDOWN_SIGNAL = 'SHUTDOWN'
DownloadFileJob = collections.namedtuple(
    'DownloadFileJob', [
        'transfer_id',
        'bucket',
        'key',
        'filename',
        'extra_args',
        'expected_size',
    ]
)
GetObjectJob = collections.namedtuple(
    'GetObjectJob', [
        'transfer_id',
        'bucket',
        'key',
        'filename',
        'extra_args',
        'offset',
        'final_filename',
    ]
)


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
        if client_kwargs is None:
            client_kwargs = {}
        self._client_factory = ClientFactory(client_kwargs)

        self._transfer_config = config
        if config is None:
            self._transfer_config = ProcessTransferConfig()

        self._submitter_queue = multiprocessing.Queue(1000)
        self._worker_queue = multiprocessing.Queue(1000)
        self._osutil = OSUtils()

        self._id_counter = 0
        self._started = False

        # These below are initialized in the start() method
        self._manager = None
        self._transfer_monitor = None
        self._submitter = None
        self._workers = []

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
        self._assert_has_started()
        if extra_args is None:
            extra_args = {}
        self._validate_all_known_args(extra_args)
        call_args = CallArgs(
            bucket=bucket, key=key, filename=filename, extra_args=extra_args,
            expected_size=expected_size)
        future = self._get_transfer_future(call_args)
        self._submitter_queue.put(
            DownloadFileJob(
                transfer_id=future.meta.transfer_id, bucket=bucket, key=key,
                filename=filename, extra_args=extra_args,
                expected_size=expected_size,
            )
        )
        return future

    def start(self):
        self._start_transfer_monitor_manager()
        self._start_submitter()
        self._start_get_object_workers()
        self._started = True

    def shutdown(self):
        """Shutdown the downloader

        It will wait till all downloads are complete before returning.
        """
        self._shutdown_submitter()
        self._shutdown_get_object_workers()
        self._shutdown_transfer_monitor_manager()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *args):
        self.shutdown()

    def _assert_has_started(self):
        if not self._started:
            raise RuntimeError(
                'The start() method must be called before calling '
                'download_file()')

    def _validate_all_known_args(self, provided):
        for kwarg in provided:
            if kwarg not in ALLOWED_DOWNLOAD_ARGS:
                raise ValueError(
                    "Invalid extra_args key '%s', "
                    "must be one of: %s" % (
                        kwarg, ', '.join(ALLOWED_DOWNLOAD_ARGS)))

    def _get_transfer_future(self, call_args):
        transfer_id = self._id_counter
        meta = ProcessPoolTransferMeta(
            call_args=call_args, transfer_id=transfer_id)
        future = ProcessPoolTransferFuture(
            monitor=self._transfer_monitor, meta=meta)
        self._id_counter += 1
        return future

    def _start_transfer_monitor_manager(self):
        self._manager = TransferMonitorManager()
        self._manager.start()
        self._transfer_monitor = self._manager.TransferMonitor()

    def _start_submitter(self):
        self._submitter = Submitter(
            transfer_config=self._transfer_config,
            client_factory=self._client_factory,
            transfer_monitor=self._transfer_monitor,
            osutil=self._osutil,
            submitter_queue=self._submitter_queue,
            worker_queue=self._worker_queue
        )
        self._submitter.start()

    def _start_get_object_workers(self):
        for _ in range(self._transfer_config.max_request_processes):
            worker = GetObjectWorker(
                queue=self._worker_queue,
                client_factory=self._client_factory,
                transfer_monitor=self._transfer_monitor,
                osutil=self._osutil,
            )
            worker.start()
            self._workers.append(worker)

    def _shutdown_transfer_monitor_manager(self):
        self._manager.shutdown()

    def _shutdown_submitter(self):
        self._submitter_queue.put(SHUTDOWN_SIGNAL)
        self._submitter.join()

    def _shutdown_get_object_workers(self):
        for _ in self._workers:
            self._worker_queue.put(SHUTDOWN_SIGNAL)
        for worker in self._workers:
            worker.join()


class ProcessPoolTransferFuture(BaseTransferFuture):
    def __init__(self, monitor, meta):
        """The future associated to a submitted process pool transfer request

        :type monitor: TransferMonitor
        :param monitor: The monitor associated to the proccess pool downloader

        :type meta: ProcessPoolTransferMetaTransferMeta
        :param meta: The metadata associated to the request. This object
            is visible to the requester.
        """
        self._monitor = monitor
        self._meta = meta

    @property
    def meta(self):
        return self._meta

    def done(self):
        return self._monitor.is_done(self._meta.transfer_id)

    def result(self):
        try:
            return self._monitor.poll_for_result(self._meta.transfer_id)
        except KeyboardInterrupt:
            self.cancel()
            raise

    def cancel(self):
        self._monitor.notify_exception(
            self._meta.transfer_id, CancelledError()
        )


class ProcessPoolTransferMeta(BaseTransferMeta):
    """Holds metadata about the TransferFuture"""
    def __init__(self, transfer_id, call_args):
        self._transfer_id = transfer_id
        self._call_args = call_args
        self._user_context = {}

    @property
    def call_args(self):
        return self._call_args

    @property
    def transfer_id(self):
        return self._transfer_id

    @property
    def user_context(self):
        return self._user_context


class ClientFactory(object):
    def __init__(self, client_kwargs=None):
        """Creates S3 clients for processes

        Botocore sessions and clients are not pickleable so they cannot be
        inherited across Process boundaries. Instead, they must be instantiated
        once a process is running.
        """
        self._client_kwargs = client_kwargs
        if self._client_kwargs is None:
            self._client_kwargs = {}

    def create_client(self):
        """Create a botocore S3 client"""
        return botocore.session.Session().create_client(
            's3', **self._client_kwargs)


class TransferMonitor(object):
    def __init__(self):
        """Monitors transfers for cross-proccess communication

        Notifications can be sent to the monitor and information can be
        retrieved from the monitor for a particular transfer. Even though
        """
        self._transfer_states = collections.defaultdict(TransferState)

    def is_done(self, transfer_id):
        return self._transfer_states[transfer_id].done

    def notify_done(self, transfer_id):
        self._transfer_states[transfer_id].set_done()

    def poll_for_result(self, transfer_id):
        self._transfer_states[transfer_id].wait_till_done()
        exception = self._transfer_states[transfer_id].exception
        if exception:
            raise exception
        return None

    def notify_exception(self, transfer_id, exception):
        """Notify an exception was encountered for a transfer

        :param transfer_id: Unique identifier for the transfer
        :param exception: The exception encountered for that transfer
        """
        # TODO: Not all exceptions are pickleable so if we are running
        # this in a  multiprocessing.BaseManager we will want to
        # make sure to update this signature to ensure pickleability of the
        # arguments or have the ProxyObject do the serialization.
        self._transfer_states[transfer_id].exception = exception

    def get_exception(self, transfer_id):
        """Retrieve the exception encountered for the transfer

        :param transfer_id: Unique identifier for the transfer
        :return: The exception encountered for that transfer. Otherwise
            if there were no exceptions, returns None.
        """
        return self._transfer_states[transfer_id].exception

    def notify_expected_jobs_to_complete(self, transfer_id, num_jobs):
        """Notify the amount of jobs expected for a transfer

        :param transfer_id: Unique identifier for the transfer
        :param num_jobs: The number of jobs to complete the transfer
        """
        self._transfer_states[transfer_id].jobs_to_complete = num_jobs

    def notify_job_complete(self, transfer_id):
        """Notify that a single job is completed for a transfer

        :param transfer_id: Unique identifier for the transfer
        :return: The number of jobs remaining to complete the transfer
        """
        return self._transfer_states[transfer_id].decrement_jobs_to_complete()


class TransferState(object):
    def __init__(self):
        self._exception = None
        self._done_event = threading.Event()
        self._job_lock = threading.Lock()
        self._jobs_to_complete = 0

    @property
    def done(self):
        return self._done_event.is_set()

    def set_done(self):
        self._done_event.set()

    def wait_till_done(self):
        self._done_event.wait(MAXINT)

    @property
    def exception(self):
        return self._exception

    @exception.setter
    def exception(self, val):
        self._exception = val

    @property
    def jobs_to_complete(self):
        return self._jobs_to_complete

    @jobs_to_complete.setter
    def jobs_to_complete(self, val):
        self._jobs_to_complete = val

    def decrement_jobs_to_complete(self):
        with self._job_lock:
            self._jobs_to_complete -= 1
            return self._jobs_to_complete


class TransferMonitorManager(multiprocessing.managers.BaseManager):
    pass


TransferMonitorManager.register('TransferMonitor', TransferMonitor)


class Submitter(multiprocessing.Process):
    def __init__(self, transfer_config, client_factory,
                 transfer_monitor, osutil, submitter_queue,
                 worker_queue):
        """Submit jobs to fulfill a download file request

        :param transfer_config: Configuration for transfers
        :param client_factory: ClientFactory for creating S3 clients
        :param transfer_monitor: Monitor for notifying and retrieving state
            of transfer
        :param osutil: OSUtils object to use for os-related behavior when
            performing the transfer.
        :param submitter_queue: Queue to retrieve download file jobs
        :param worker_queue: Queue to submit GetObjectJobs for workers
            to perform
        """
        super(Submitter, self).__init__()
        self._transfer_config = transfer_config
        self._client_factory = client_factory
        self._transfer_monitor = transfer_monitor
        self._osutil = osutil
        self._submitter_queue = submitter_queue
        self._worker_queue = worker_queue
        self._client = None

    def run(self):
        # Client are not pickleable so their instantiation cannot happen
        # in the __init__ for processes that are created under the
        # spawn method.
        self._client = self._client_factory.create_client()
        while True:
            download_file_job = self._submitter_queue.get()
            if download_file_job == SHUTDOWN_SIGNAL:
                return
            try:
                self._submit_get_object_jobs(download_file_job)
            except Exception as e:
                self._transfer_monitor.notify_exception(
                    download_file_job.transfer_id, e)
                self._transfer_monitor.notify_done(
                    download_file_job.transfer_id)

    def _submit_get_object_jobs(self, download_file_job):
        size = self._get_size(download_file_job)
        temp_filename = self._allocate_temp_file(download_file_job, size)
        if size < self._transfer_config.multipart_threshold:
            self._submit_single_get_object_job(
                download_file_job, temp_filename)
        else:
            self._submit_ranged_get_object_jobs(
                download_file_job, temp_filename, size)

    def _get_size(self, download_file_job):
        expected_size = download_file_job.expected_size
        if expected_size is None:
            expected_size = self._client.head_object(
                Bucket=download_file_job.bucket, Key=download_file_job.key,
                **download_file_job.extra_args)['ContentLength']
        return expected_size

    def _allocate_temp_file(self, download_file_job, size):
        temp_filename = self._osutil.get_temp_filename(
            download_file_job.filename
        )
        self._osutil.truncate(temp_filename, size)
        return temp_filename

    def _submit_single_get_object_job(self, download_file_job, temp_filename):
        self._transfer_monitor.notify_expected_jobs_to_complete(
            download_file_job.transfer_id, 1)
        self._submit_get_object_job(
            transfer_id=download_file_job.transfer_id,
            bucket=download_file_job.bucket,
            key=download_file_job.key,
            filename=temp_filename,
            offset=0,
            extra_args=download_file_job.extra_args,
            final_filename=download_file_job.filename
        )

    def _submit_ranged_get_object_jobs(self, download_file_job, temp_filename,
                                       size):
        part_size = self._transfer_config.multipart_chunksize
        num_parts = calculate_num_parts(size, part_size)
        self._transfer_monitor.notify_expected_jobs_to_complete(
            download_file_job.transfer_id, num_parts)
        for i in range(num_parts):
            offset = i * part_size
            range_parameter = calculate_range_parameter(
                part_size, i, num_parts)
            get_object_kwargs = {'Range': range_parameter}
            get_object_kwargs.update(download_file_job.extra_args)
            self._submit_get_object_job(
                transfer_id=download_file_job.transfer_id,
                bucket=download_file_job.bucket,
                key=download_file_job.key,
                filename=temp_filename,
                offset=offset,
                extra_args=get_object_kwargs,
                final_filename=download_file_job.filename,
            )

    def _submit_get_object_job(self, **get_object_job_kwargs):
        self._worker_queue.put(GetObjectJob(**get_object_job_kwargs))


class GetObjectWorker(multiprocessing.Process):
    _MAX_ATTEMPTS = 5
    _IO_CHUNKSIZE = 2 * MB

    def __init__(self, queue, client_factory, transfer_monitor, osutil):
        """Fulfills GetObjectJobs

        Downloads the S3 object, writes it to the specified file, and
        renames the file to its final location if it completes the final
        job for a particular transfer.

        :param queue: Queue for retrieving GetObjectJob's
        :param client_factory: ClientFactory for creating S3 clients
        :param transfer_monitor: Monitor for notifying
        :param osutil: OSUtils object to use for os-related behavior when
            performing the transfer.
        """
        super(GetObjectWorker, self).__init__()
        self._queue = queue
        self._client_factory = client_factory
        self._transfer_monitor = transfer_monitor
        self._osutil = osutil
        self._client = None

    def run(self):
        # Client are not pickleable so their instantiation cannot happen
        # in the __init__ for processes that are created under the
        # spawn method.
        self._client = self._client_factory.create_client()
        while True:
            job = self._queue.get()
            if job == SHUTDOWN_SIGNAL:
                return
            if not self._transfer_monitor.get_exception(job.transfer_id):
                self._run_get_object_job(job)
            remaining = self._transfer_monitor.notify_job_complete(
                job.transfer_id)
            if not remaining:
                self._finalize_download(
                    job.transfer_id, job.filename, job.final_filename
                )

    def _run_get_object_job(self, job):
        try:
            self._do_get_object(
                bucket=job.bucket, key=job.key, filename=job.filename,
                extra_args=job.extra_args, offset=job.offset
            )
        except Exception as e:
            self._transfer_monitor.notify_exception(job.transfer_id, e)

    def _do_get_object(self, bucket, key, extra_args, filename, offset):
        last_exception = None
        for i in range(self._MAX_ATTEMPTS):
            try:
                response = self._client.get_object(
                    Bucket=bucket, Key=key, **extra_args)
                self._write_to_file(filename, offset, response['Body'])
                return
            except S3_RETRYABLE_DOWNLOAD_ERRORS as e:
                logger.debug('Retrying exception caught (%s), '
                             'retrying request, (attempt %s / %s)', e, i,
                             self._MAX_ATTEMPTS, exc_info=True)
                last_exception = e
        raise RetriesExceededError(last_exception)

    def _write_to_file(self, filename, offset, body):
        with open(filename, 'rb+') as f:
            f.seek(offset)
            chunks = iter(lambda: body.read(self._IO_CHUNKSIZE), b'')
            for chunk in chunks:
                f.write(chunk)

    def _finalize_download(self, transfer_id, filename, final_filename):
        if self._transfer_monitor.get_exception(transfer_id):
            self._osutil.remove_file(filename)
        else:
            self._do_file_rename(transfer_id, filename, final_filename)
        self._transfer_monitor.notify_done(transfer_id)

    def _do_file_rename(self, transfer_id, filename, final_filename):
        try:
            self._osutil.rename_file(filename, final_filename)
        except Exception as e:
            self._transfer_monitor.notify_exception(transfer_id, e)
            self._osutil.remove_file(filename)
