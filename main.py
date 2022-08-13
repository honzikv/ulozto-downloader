import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor

import logging
from typing import List
from concurrent.futures import wait

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.level = logging.DEBUG


def worker_fn(link: str, workers: int, python_path: str, output_dir: str):
    """
    Worker function for multiprocessing.
    """
    logger.info(f"Worker function for link: \"{link}\" started. Workers: {workers}, download dir: {output_dir}")
    # subprocess.call((python_path, f"ulozto-downloader.py", f"--parts {workers}", f"--output {output_dir}", f"{link}"))
    subprocess.call(f'{python_path} ulozto-downloader.py --parts {workers} --output {output_dir} {link}', shell=True)
    logger.info(f"Worker function for link \"{link}\" finished.")


def read_links(file_path: str) -> List[str]:
    """
    Read links from file
    Args: file path
    Returns: list of links
    """
    with open(file_path, "r") as f:
        return f.read().splitlines()


def main(args: List[str]):
    """
    Main of the script
    Args:
        1st argument - file with links - links are separated by newline
        2nd argument - max number of simultaneous downloads - i.e. how many processes of ulozto-downloader
        will be run
        3rd argument - max number of threads in ulozto-downloader
        4th argument - python executable path
        5th argument - output dir - by default downloads is used
        In total args[1] * args[2] threads will be run (apart from main)
    Returns:
    """

    # Parse the args
    download_list_path = args[0]
    max_concurrent_downloads = int(args[1])
    workers_per_task = int(args[2])
    python_path = args[3]
    output_dir = args[4] if len(args) > 4 else os.path.join(os.getcwd(), "downloads")
    os.makedirs(output_dir, exist_ok=True)

    # Log the args
    logger.info(f"Download list path: {download_list_path}")
    logger.info(f"Max concurrent downloads: {max_concurrent_downloads}")

    executor = ThreadPoolExecutor(max_workers=max_concurrent_downloads)
    links = read_links(download_list_path)

    futures = [executor.submit(worker_fn, link, workers_per_task, python_path, output_dir) for
               link in links]
    logger.info(f"{len(futures)} futures created. Waiting till they are finished")
    wait(futures)


if __name__ == "__main__":
    main(sys.argv[1:])
    logger.info("All futures finished")
    sys.exit(0)
