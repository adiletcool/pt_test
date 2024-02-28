import asyncio
import json
import os
import sys
from asyncio import Task
from functools import partial
from multiprocessing import Manager, Queue, Pool, Process

from loguru import logger
from uvloop import EventLoopPolicy

from sample.generate import generate_data
from src.client import send_request
from src.config import Config
from src.model import FilterQuery


def get_chunks_positions(config: Config) -> set[tuple[int, int]]:
    """
    Returns set of positions (start, end) of chunks, so that we can distribute them by processes.
    (Positions need to set pointer when reading file)
    """
    file_size = os.path.getsize(config.path)
    file_chunk_size = file_size // config.cpu_count
    ram_chunk_size = (config.ram * 1024 ** 8) // config.cpu_count
    chunk_size = min(file_chunk_size, ram_chunk_size)

    chunk_args = set()

    with open(config.path, 'r') as f:
        def is_start_of_line(position):
            if position == 0:
                return True
            f.seek(position - 1)
            return f.read(1) == '\n'

        def get_next_line_position(position):
            f.seek(position)  # set position
            f.readline()  # Read the current line till the end
            return f.tell()  # return position after reading line

        chunk_start = 0
        while chunk_start < file_size:
            chunk_end = min(file_size, chunk_start + chunk_size)

            while not is_start_of_line(chunk_end):
                chunk_end -= 1

            if chunk_start == chunk_end:
                chunk_end = get_next_line_position(chunk_end)

            args = (chunk_start, chunk_end)
            chunk_args.add(args)

            chunk_start = chunk_end  # Move to the next chunk
    return chunk_args


def process_chunk(
    filename: str,
    filter_query: FilterQuery | None,
    filtered_data_queue: Queue,
    skipped_data_queue: Queue,
    position_start: int,
    position_end: int,
):
    """
    Process lines within the chunk with defined positions and puts them into the queue
    """

    def _check_line() -> bool:
        """ Check if cli filter can be applied to the line (json) """
        value = data

        for i, key in enumerate(filter_query.keys):
            value = value.get(key)

            if i == len(filter_query.keys) - 1:
                if value is None:
                    return False
                if value == filter_query.value:
                    return True
                return False
            elif value is None or type(value) is not dict:
                return False

    with open(filename, 'r') as f:
        f.seek(position_start)  # set position

        for line in f:
            position_start += len(line)
            data = json.loads(line)

            if not filter_query or _check_line():
                filtered_data_queue.put(line)
            else:
                skipped_data_queue.put(line)

            if position_start >= position_end:
                break


async def _process_filtered_data_queue(filtered_data_queue: Queue, config: Config):
    """
    Receives lines from the queue and puts them into the list.
    If this list exceeds the maximum length (cli limit),
    then creates asyncio task to send request to http server.
    """
    filtered_data_count = 0
    filtered_data_size = 0
    filtered_data_to_send = []
    send_data_tasks: list[Task] = []

    while True:
        line: str = filtered_data_queue.get()

        if line == 'END':
            break

        filtered_data_count += 1
        filtered_data_size += sys.getsizeof(line)
        filtered_data_to_send.append(line)

        if len(filtered_data_to_send) == config.limit:
            # create and run task
            send_data_tasks.append(
                asyncio.create_task(send_request(config.url, filtered_data_to_send))
            )
            filtered_data_to_send = []

    # Send remaining data
    if len(filtered_data_to_send) > 0:
        send_data_tasks.append(
            asyncio.create_task(send_request(config.url, filtered_data_to_send))
        )

    # await remaining tasks
    await asyncio.gather(*send_data_tasks)
    logger.info(f'Passed: {filtered_data_count} lines, {filtered_data_size} bytes')


def process_filtered_data_queue(filtered_data_queue: Queue, config: Config):
    asyncio.set_event_loop_policy(EventLoopPolicy())
    return asyncio.run(_process_filtered_data_queue(filtered_data_queue, config))


def process_skipped_data_queue(skipped_data_queue: Queue, config: Config):
    skipped_data_count = 0
    skipped_data_size = 0

    with open(config.output, 'w') as f:
        while True:
            line: str = skipped_data_queue.get()
            if line == 'END':
                break
            skipped_data_count += 1
            skipped_data_size += sys.getsizeof(line)
            f.write(line)
            f.flush()

    logger.info(f'Skipped: {skipped_data_count} lines, {skipped_data_size} bytes')


def main():
    logger.info('Application started')
    config = Config()
    logger.info(config)

    logger.info('Generating random data')
    generate_data(n_lines=1_000)  # TODO: remove in prod

    with Manager() as manager:
        filtered_data_queue: Queue = manager.Queue(config.limit)
        skipped_data_queue: Queue = manager.Queue()

        chunks_positions = get_chunks_positions(config)

        process_chunk_func = partial(
            process_chunk,
            config.path,
            config.filter_query,
            filtered_data_queue,
            skipped_data_queue,
        )

        process_filtered_data = Process(
            target=process_filtered_data_queue,
            args=(filtered_data_queue, config)
        )
        process_skipped_data = Process(
            target=process_skipped_data_queue,
            args=(skipped_data_queue, config)
        )

        process_filtered_data.start()
        process_skipped_data.start()

        with Pool(processes=config.cpu_count) as pool:
            pool.starmap(process_chunk_func, chunks_positions)

        pool.join()

        filtered_data_queue.put('END')
        skipped_data_queue.put('END')

        process_filtered_data.join()
        process_skipped_data.join()
        logger.info('Application finished')


if __name__ == '__main__':
    main()
