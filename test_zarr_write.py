import os
import time
import s3fs
import zarr
import argparse
import configparser
import logging
import pandas as pd
import numpy as np
import multiprocessing as mp
from functools import partial
from pathlib import Path
from rich.progress import track

logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
logger.addHandler(handler)


class Timer:
    def __enter__(self):
        self.start = time.time()

    def __exit__(self, exc_type, exc_value, exc_tb):
        self.end = time.time()
        self.duration = self.end - self.start


def write_random_file(file_name: str, s3: s3fs.S3FileSystem, size: int = 1024):
    logger.debug(f"Create file {file_name}")

    timer = Timer()
    with timer:
        with s3.open(file_name, "wb") as f:
            f.write(os.urandom(size))

    logger.debug(f"Done writing file {file_name}")
    return timer.duration


def read_file(file_name: str, s3: s3fs.S3FileSystem):
    logger.debug(f"Reading file {file_name}")
    timer = Timer()
    with timer:
        with s3.open(file_name, "rb") as f:
            contents = f.read()
    logger.debug(f"Done reading file {file_name}")
    return timer.duration


def write_test(
    s3: s3fs.S3FileSystem,
    bucket_name: str,
    file_size: int = 1024,
    n_samples: int = 10,
    n_workers: int = 4,
):
    logger.info(f"Running write test with {n_samples} samples and {n_workers} workers")
    _do_write = partial(write_random_file, s3=s3, size=file_size)
    file_names = [f"{bucket_name}/test_file_{i}.bin" for i in range(n_samples)]

    timer = Timer()
    with timer:
        with mp.Pool(n_workers) as pool:
            times = list(track(pool.imap(_do_write, file_names), total=len(file_names)))
            times = np.array(times)

    total_duration = timer.duration
    return dict(total_duration=total_duration, times=times)


def read_test(
    s3: s3fs.S3FileSystem,
    bucket_name: str,
    n_samples: int = 10,
    n_workers: int = 4,
):
    logger.info(f"Running read test with {n_samples} samples and {n_workers} workers")
    _do_read = partial(read_file, s3=s3)
    file_names = [f"{bucket_name}/test_file_{i}.bin" for i in range(n_samples)]
    file_names = list(reversed(file_names))

    timer = Timer()
    with timer:
        with mp.Pool(n_workers) as pool:
            times = list(track(pool.imap(_do_read, file_names), total=len(file_names)))
            times = np.array(times)

    total_duration = timer.duration
    return dict(total_duration=total_duration, times=times)


def do_test(
    s3: s3fs.S3FileSystem, name: str, n_samples: int, n_workers: int, file_size: int
):
    total_size = file_size * n_samples

    result = write_test(
        s3, name, file_size=file_size, n_samples=n_samples, n_workers=n_workers
    )

    total_duration = result["total_duration"]
    times = result["times"]
    avg_write_time = times.mean()
    throughput = total_size / total_duration

    write_result = {
        "write_time_total": total_duration,
        "write_time_avg": avg_write_time,
        "write_throughput": throughput,
    }

    logger.info(f"Average write time {avg_write_time}")
    logger.info(f"Total Time taken {total_duration}")
    logger.info(f"Througput (bytes/s) {throughput}")

    time.sleep(1)

    result = read_test(s3, name, n_samples=n_samples, n_workers=n_workers)

    total_duration = result["total_duration"]
    times = result["times"]
    avg_read_time = times.mean()
    throughput = total_size / total_duration

    read_result = {
        "read_time_total": total_duration,
        "read_time_avg": avg_read_time,
        "read_throughput": throughput,
    }

    logger.info(f"Time taken {total_duration}")
    logger.info(f"Average read time {avg_read_time}")
    logger.info(f"Througput (bytes/s) {throughput}")

    results = {}
    results.update(write_result)
    results.update(read_result)
    return results


def grid_scan(s3: s3fs.S3FileSystem, name: str, output_file: str):
    n_samples = 100
    file_size = 1024

    n_workers_scan = [1, 2, 4, 6, 8, 10, 12]

    results = []
    for n_workers in n_workers_scan:
        metrics = do_test(s3, name, n_samples, n_workers, file_size)
        metrics["n_workers"] = n_workers
        metrics["n_samples"] = n_samples
        metrics["file_size"] = file_size
        results.append(metrics)

    df = pd.DataFrame(results)
    df.to_csv(output_file)


def main():
    parser = argparse.ArgumentParser(
        prog="S3 Test",
        description="Test latency and bandwidth of an S3 endpoint",
    )

    parser.add_argument("config_file")
    parser.add_argument("bucket_name")
    parser.add_argument("output_file")
    args = parser.parse_args()

    config = configparser.ConfigParser()
    with open(Path(args.config_file).expanduser()) as stream:
        config.read_string("[DEFAULT]\n" + stream.read())
        config = config["DEFAULT"]

    key = config["access_key"]
    secret = config["secret_key"]
    url = f"https://{config['host_base']}"
    name = args.bucket_name

    s3 = s3fs.S3FileSystem(anon=False, key=key, secret=secret, endpoint_url=url)
    grid_scan(s3, name, args.output_file)


if __name__ == "__main__":
    # Important! Must change multiprocessing spawning method or s3fs will hang in new processes.
    # See https://s3fs.readthedocs.io/en/latest/index.html?highlight=multiprocessing#multiprocessing
    mp.set_start_method("spawn", force=True)
    main()
