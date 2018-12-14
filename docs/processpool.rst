Process Pool Downloader
=======================

The ``ProcessPoolDownloader`` speeds up throughput of S3 downloads
by using processes instead of threads to download S3 objects.

Getting Started
---------------

The ``ProcessPoolDownloader`` can be used to download a single file:


.. code:: python

     from s3transfer.processpool import ProcessPoolDownloader

     with ProcessPoolDownloader() as downloader:
          downloader.download_file('mybucket', 'mykey', 'myfile')

This snippet downloads the S3 object located in the bucket ``mybucket`` at the
key ``mykey`` to the local file ``myfile``. Any errors encountered during the
transfer will are not propagated. To determine if a transfer succeeded or
failed, use the `Futures`_ interface.


The ``ProcessPoolDownloader`` can be used to download multiple files as well:

.. code:: python

     from s3transfer.processpool import ProcessPoolDownloader

     with ProcessPoolDownloader() as downloader:
          downloader.download_file('mybucket', 'mykey', 'myfile')
          downloader.download_file('mybucket', 'myotherkey', 'myotherfile')

When running this snippet, the downloading of ``mykey`` and ``myotherkey``
happens in parallel. The first ``download_file()`` call does not block the
second ``download_file()`` call. The snippet blocks when exiting
the context manager and blocks until both downloads are complete.

Alternatively if you do not want to use a context manager, you can explicitly
call the ``ProcessPoolDownloader.start()`` and
``ProcessPoolDownloader.shutdown()`` methods:

.. code:: python

     from s3transfer.processpool import ProcessPoolDownloader

     downloader = ProcessPoolDownloader()
     downloader.start()
     downloader.download_file('mybucket', 'mykey', 'myfile')
     downloader.download_file('mybucket', 'myotherkey', 'myotherfile')
     downloader.shutdown()

For this code snippet, the ``shutdown()`` method call blocks until both
downloads are complete.

Additional Parameters
---------------------

These are the additional parameters that you can provide to the
``download_file()`` command:

* ``extra_args``: A dictionary containing any additional client arguments
  to include in the
  `GetObject <https://botocore.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.get_object>`_
  API request. For example:

  .. code:: python

     from s3transfer.processpool import ProcessPoolDownloader

     with ProcessPoolDownloader() as downloader:
          downloader.download_file(
               'mybucket', 'mykey', 'myfile',
               extra_args={'VersionId': 'myversion'})


* ``expected_size``: By default, the downloader will make an HeadObject
  call to determine the size of the object. To opt-out of this additional
  API call, you can provide the size yourself:

  .. code:: python

     from s3transfer.processpool import ProcessPoolDownloader

     with ProcessPoolDownloader() as downloader:
          downloader.download_file(
               'mybucket', 'mykey', 'myfile', expected_size=100)

Futures
-------

When the ``download_file()`` method is called, it immediately returns a
``ProcessPoolTransferFuture``. The future is useful to retrieve and poll
for the state of a particular transfer. To get the result of download, call
the ``result()`` of the future. The ``result()`` method blocks until the
transfer completes, whether it succeeds or fails. For example:

.. code:: python

     from s3transfer.processpool import ProcessPoolDownloader

     with ProcessPoolDownloader() as downloader:
          future = downloader.download_file('mybucket', 'mykey', 'myfile')
          print(future.result())

If the download succeeds, the future returns ``None``:

.. code:: python

     None


If the download fails, the exception is raised from the ``result()`` call.
For example, if ``mykey`` did not exist, the following error would be raised


.. code:: python

     botocore.exceptions.ClientError: An error occurred (404) when calling the HeadObject operation: Not Found


It is important to note that ``Future.result()`` can only be called while the
``ProcessPoolDownloader`` is running (e.g. before ``shutdown()`` is called or
inside the context manager).

Process Pool Configuration
--------------------------

By default, the downloader has the following configuration options:

* ``multipart_threshold``: The threshold size for performing ranged downloads.
  By default, ranged downloads happen for S3 objects that are greater than
  or equal to 8 MB in size.

* ``multipart_chunksize``: The size of each ranged download. By default, the
  size of each ranged download is 8 MB.

* ``max_request_processes``: The number of processes in the pool to download
  S3 objects. By default, there are 10 processes in the pool.


To change the default configuration, you can use the ``ProcessTransferConfig``
to update these configuration values:

.. code:: python

     from s3transfer.processpool import ProcessPoolDownloader
     from s3transfer.processpool import ProcessTransferConfig

     config = ProcessTransferConfig(
          multipart_threshold=64 * 1024 * 1024,  # 64 MB
          max_request_processes=50
     )
     downloader = ProcessPoolDownloader(config=config)


Client Configuration
--------------------

The process pool downloader creates ``botocore`` clients on your behalf. In
order to affect how the client is created, you can pass the keyword arguments
that would have been used in the ``botocore.Session.create_client()`` call:

.. code:: python


     from s3transfer.processpool import ProcessPoolDownloader
     from s3transfer.processpool import ProcessTransferConfig

     downloader = ProcessPoolDownloader(
          client_kwargs={'region_name': 'us-west-2'})


This snippet ensures that all clients created by the ``ProcessPoolDownloader``
are using ``us-west-2`` as their region.
