import s3fs
import json
import kerchunk.hdf
import zarr
import xarray as xr
from pathlib import Path

import zarr.convenience


def wipe_attrs(attrs):
    for k, v in attrs.items():
        attrs[k] = ""


def write_kerchunk(file_name: str):
    ENDPOINT_URL = "https://s3.echo.stfc.ac.uk"
    fs = s3fs.S3FileSystem(anon=True, endpoint_url=ENDPOINT_URL)
    url = f"s3://mast/test/perf/{file_name}"
    with fs.open(url) as infile:
        h5chunks = kerchunk.hdf.SingleHdf5ToZarr(infile, url, inline_threshold=100)
        meta = h5chunks.translate()

    with Path(f"{file_name}.json").open("w") as handle:
        json.dump(meta, handle)


def convert_group(zarr_file: str, group_name: str):
    ds = xr.open_zarr(zarr_file, group=group_name)
    ds.attrs = {}
    for k, v in ds.data_vars.items():
        v.attrs = {}
    ds.to_zarr("data/30420.zarr", group=group_name, consolidated=False, mode="a")
    ds.to_netcdf("data/30420.h5", group=group_name, mode="a")
    ds.to_netcdf("data/30420.nc", group=group_name, mode="a")


def main():
    zarr_file = Path("30420.zarr")
    convert_group(zarr_file, "rbb")
    convert_group(zarr_file, "efm")
    write_kerchunk("data/30420.h5")
    write_kerchunk("data/30420.nc")
    zarr.convenience.consolidate_metadata("data/30420.zarr")


if __name__ == "__main__":
    main()
