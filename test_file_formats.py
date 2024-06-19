import json
import logging
import pandas as pd
import itertools
import s3fs
import xarray as xr
from pathlib import Path
from utils import Timer

logger = logging.getLogger()
logger.setLevel(logging.INFO)

ENDPOINT_URL = "https://s3.echo.stfc.ac.uk"


def get_fs():
    s3 = s3fs.S3FileSystem(anon=True, endpoint_url=ENDPOINT_URL)
    return s3


def run_test(file_name, group_name, load, kerchunk):
    fs = get_fs()
    timer = Timer()
    with timer:
        ds = read_dataset(file_name, group_name, fs=fs, load=load, kerchunk=kerchunk)
    duration = timer.duration
    nbytes = ds.nbytes
    throughput = nbytes / duration
    return dict(
        url=file_name,
        group=group_name,
        duration=duration,
        nbytes=nbytes,
        throughput=throughput,
        test_type="load" if load else "open",
        kerchunk=kerchunk,
    )


def read_kerchunk(file_name: str, group_name: str, fs):
    rpath = f"{file_name}.json"
    with fs.open(rpath) as f:
        references = json.loads(f.read())

    ds = xr.open_dataset(
        "reference://",
        group=group_name,
        engine="zarr",
        backend_kwargs={
            "consolidated": False,
            "storage_options": {
                "fo": references,
                "remote_protocol": "s3",
                "remote_options": {"anon": True, "endpoint_url": ENDPOINT_URL},
            },
        },
    )
    return ds


def read_dataset(
    file_name: str,
    group_name: str,
    fs: s3fs.S3FileSystem,
    load: bool = False,
    kerchunk: bool = False,
):
    if file_name.endswith("zarr"):
        ds = xr.open_zarr(fs.get_mapper(file_name), consolidated=True, group=group_name)
        if load:
            name = "plasma_current_rz" if group_name == "efm" else "data"
            ds[name].compute()
    else:
        if kerchunk:
            ds = read_kerchunk(file_name, group_name, fs)
            if load:
                name = "plasma_current_rz" if group_name == "efm" else "data"
                ds[name].compute()
        else:
            engine = "h5netcdf"
            with fs.open(file_name) as handle:
                ds = xr.open_dataset(handle, group=group_name, engine=engine)
                if load:
                    name = "plasma_current_rz" if group_name == "efm" else "data"
                    ds[name].compute()
    return ds


def main():
    urls = [
        "s3://mast/test/perf/data/30420.nc",
        "s3://mast/test/perf/data/30420.zarr",
        "s3://mast/test/perf/data/30420.h5",
    ]
    groups = ["efm", "rbb"]
    load = [False, True]
    kerchunk = [True, False]

    test_params = itertools.product(urls, groups, load, kerchunk)
    test_params = list(test_params)
    results = []

    for i in range(10):
        for params in test_params:
            logging.info(f"iteration: {i}, {params}")
            result = run_test(*params)
            result["iteration"] = i
            results.append(result)

    df = pd.DataFrame(results)
    df["file_type"] = df.url.map(lambda x: Path(x).suffix[1:])
    df.to_csv("results/file_format_results.csv")


if __name__ == "__main__":
    main()
