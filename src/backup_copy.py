#!/usr/bin/env python3
import argparse
import logging
import queue
import sys

from multiprocessing import Queue, Event
from time import sleep
from typing import Dict, Callable
from urllib.parse import urlparse

from boto3 import Session

from lib.processor import Processor
from lib.thread_with_result import ThreadWithResult, ThreadResult


logging.basicConfig(
    level=logging.INFO, stream=sys.stderr,
    format='%(asctime)s %(levelname)s %(message)s',
)

logger = logging.getLogger('backup-copy')


def exception_wrapper(func: Callable) -> Callable:
    def wrapper(*args, **kwargs) -> ThreadResult:
        # try to extract the processor from the first argument
        processor = args[0] if len(args) > 0 else None
        try:
            func_result = ThreadResult(is_exception=False, result=func(*args, **kwargs))
        except Exception as ex:
            func_result = ThreadResult(is_exception=True, result=ex)

        if isinstance(processor, Processor):
            processor.set_stopping()

        return func_result

    return wrapper


@exception_wrapper
def files_to_copy(processor: Processor, source_path: str):
    boto_session = Session()
    url_parts = urlparse(source_path)
    logger.debug(f"Parsing S3 URL - Bucket: {url_parts.netloc} - Path: {url_parts.path.lstrip('/')}")

    s3_client = boto_session.client('s3')
    prefix_queue = [url_parts.path.lstrip('/')]

    while len(prefix_queue) > 0:
        prefix = prefix_queue.pop()

        is_truncated = True
        continuation_token = None
        while is_truncated:
            list_options = {
                'Bucket': url_parts.netloc,
                'Delimiter': '/',
                'MaxKeys': 1000,
                'Prefix': prefix
            }

            if continuation_token is not None:
                list_options['ContinuationToken'] = continuation_token

            response = s3_client.list_objects_v2(**list_options)

            for folder in response.get('CommonPrefixes', {}):
                prefix_queue.append(folder["Prefix"])

            for content in response.get('Contents', {}):
                logger.info(f"Adding {content['Key']} to Copy Queue")
                processor.queue().put({'Bucket': url_parts.netloc, 'Key': content['Key']})

            is_truncated = response.get('IsTruncated', False)
            continuation_token = response.get('NextContinuationToken', None)

    # we're done, set the flag
    logger.info(f"Source is Empty: Setting Stop Flag")
    processor.set_stopping()


@exception_wrapper
def copy_to_destination(processor: Processor, destination_path: str):
    # instantiate a boto session per thread
    session = Session()
    s3_client = session.client('s3')

    destination_parts = urlparse(destination_path)
    while True:
        try:
            source_object = processor.queue().get(timeout=10)
            prefix = f"{destination_parts.path.lstrip('/')}/" if destination_parts.path else ''

            destination_key = f"{prefix}{source_object['Key']}"
            # we've hit a folder, create it
            if destination_key.endswith('/'):
                logger.info(f"Creating Folder: {source_object['Key']} in {destination_parts.netloc}")
                if not processor.dry_run():
                    s3_client.put_object(
                        Bucket=destination_parts.netloc,
                        Key=destination_key
                    )
            # this is a file, copy it
            else:
                logger.info(
                    f"Copying Object {source_object['Key']} to {destination_key}"
                )
                if not processor.dry_run():
                    s3_client.copy_object(
                        Bucket=destination_parts.netloc,
                        CopySource=source_object,
                        Key=destination_key
                    )
        except queue.Empty:
            if processor.is_stopping():
                return
            else:
                sleep(1)


def backup_copy(args: Dict) -> int:
    logger.info(f"Starting backup-copy")
    logger.info(
        f"Source: {args['source']} - Destination(s): {', '.join(args['destinations'])} - Threads: {args['max_threads']}"
    )

    queue_size = args['max_threads'] * 5
    logger.debug(f"Using Queue Size: {queue_size}")

    session = Session()
    s3_client = session.client('s3')

    for destination in args['destinations']:
        logger.info(f"Copying from Source: {args['source']} to Destination: {destination}")
        copy_processor = Processor(Queue(queue_size), Event(), args['dry'])

        # parse the destination uri to ensure we don't need to create any "subfolders" first
        destination_url = urlparse(destination)
        destination_path = ''
        for p in (p for p in destination_url.path.split('/') if p):
            destination_path += f'{p}/'
            logger.info(f"Creating Path: {destination_path} in Destination Bucket: {destination}")
            if not copy_processor.dry_run():
                s3_client.put_object(
                    Bucket=destination_url.netloc,
                    Key=destination_path
                )

        logger.info(f"Starting Source Thread")
        source_thread = ThreadWithResult(target=files_to_copy, args=(copy_processor, args['source']))
        source_thread.start()

        destination_threads = []
        for i in range(args['max_threads']):
            logger.info(f"Starting Destination Thread: {i}")
            destination_thread = ThreadWithResult(target=copy_to_destination, args=(copy_processor, destination))
            destination_threads.append(destination_thread)
            destination_thread.start()

        # join the source thread, waiting for it to complete
        logger.debug("Waiting for Source Thread to Complete")
        source_thread.join()
        source_result = source_thread.result

        # join and collect results from destination threads
        thread_results = {}
        for thread in destination_threads:
            logger.debug(f"Waiting for Destination Thread {thread.name} to Complete")
            thread.join()
            thread_results[thread.name] = thread.result

        exception_count = 0
        if source_result.is_exception:
            logger.error(f"Exception Thrown in Source Thread: {source_result.result}")
            exception_count += 1

        for thread_name, thread_result in thread_results.items():
            if thread_result.is_exception:
                logger.error(f"Exception Thrown in Destination Thread {thread_name}: {thread_result.result}")
                exception_count += 1

        return exception_count


if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--source', help="source copy path", required=True)
    arg_parser.add_argument('--destinations', help="destination copy paths", nargs='+', required=True)
    arg_parser.add_argument('--max-threads', help="number of threads to process uploads", type=int, default=10)
    arg_parser.add_argument('--dry', help="do a dry run, don't copy anything", action='store_true', default=False)

    exit(backup_copy(vars(arg_parser.parse_args())))
