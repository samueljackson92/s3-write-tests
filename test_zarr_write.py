import os
import time
import s3fs
import zarr
import argparse
import configparser
import logging
from functools import partial
from pathlib import Path
import multiprocessing as mp

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
logger.addHandler(handler)


def create_random_file(file_name: str, s3: s3fs.S3FileSystem, size: int = 1024):
    logger.info(f"Create file {file_name}")
    with s3.open(file_name, "wb") as f:
        logger.debug("Opened")
        f.write(os.urandom(size))

    logger.info(f"Done file {file_name}")
    return


def write_zarr_data(name: str, key: str, secret: str, url: str):
    s3 = s3fs.S3FileSystem(anon=False, key=key, secret=secret, endpoint_url=url)
    store = zarr.storage.FSStore(f"s3://{name}/my_file.zarr", fs=s3)
    root = zarr.group(store=store, overwrite=True)
    s = time.time()
    for i in range(10):
        z = root.zeros(f"{i}", shape=(30000), dtype="float32")
        z[:] = 42.0
    e = time.time()
    print(f"\tWrite time {e-s:.2f}")
    store.close()


def read_zarr_data(name: str, key: str, secret: str, url: str):
    s3 = s3fs.S3FileSystem(anon=False, key=key, secret=secret, endpoint_url=url)
    store = zarr.storage.FSStore(f"s3://{name}/my_file.zarr", fs=s3)
    root = zarr.open(store=store)
    s = time.time()
    for i in range(10):
        arr = root[f"{i}"][:]
    e = time.time()
    print(f"\tRead time {e-s:.2f}")


def main():
    parser = argparse.ArgumentParser(
        prog="S3 Test",
        description="Test latency and bandwidth of an S3 endpoint",
    )

    parser.add_argument("config_file")
    parser.add_argument("bucket_name")
    args = parser.parse_args()

    config = configparser.ConfigParser()
    with open(Path(args.config_file).expanduser()) as stream:
        config.read_string("[DEFAULT]\n" + stream.read())
        config = config["DEFAULT"]

    key = config["access_key"]
    secret = config["secret_key"]
    url = f"https://{config['host_base']}"
    name = args.bucket_name
    n_samples = 100
    n_workers = 1

    s3 = s3fs.S3FileSystem(anon=False, key=key, secret=secret, endpoint_url=url)
    _do_write = partial(create_random_file, s3=s3)
    file_names = [f"{name}/test_file_{i}.bin" for i in range(n_samples)]

    with mp.Pool(n_workers) as pool:
        s = time.time()
        list(pool.imap(_do_write, file_names))
        # _do_write(file_names[0])
        e = time.time()
        logger.info(f"Time taken {e - s}")

    # path = Path("large_files")
    # path.mkdir(exist_ok=True)
    # for i in range(10):
    #     create_random_file(path / f"test_file_{i}.dump", 1024 * 100000)

    # logging.info("Test writing")
    # write_zarr_data(name, key, secret, url)
    # logging.info("Done writing")

    # logging.info("Test reading")
    # read_zarr_data(name, key, secret, url)
    # logging.info("Done reading")


if __name__ == "__main__":
    main()
