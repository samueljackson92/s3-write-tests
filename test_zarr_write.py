import numpy as np
import time
import s3fs
import zarr

def write_data(name: str, key: str, secret: str, url: str):
    s3 = s3fs.S3FileSystem(anon=False, key=key, secret=secret, endpoint_url=url)
    store = zarr.storage.FSStore(f's3://{name}/my_file.zarr', fs=s3)
    root = zarr.group(store=store, overwrite=True)
    s = time.time()
    for i in range(10):
        z = root.zeros(f'{i}', shape=(30000), dtype='float32')
        z[:] = 42.0
    e = time.time()
    print(f"\tWrite time {e-s:.2f}")
    store.close()

def read_data(name: str, key: str, secret: str, url: str):
    s3 = s3fs.S3FileSystem(anon=False, key=key, secret=secret, endpoint_url=url)
    store = zarr.storage.FSStore(f's3://{name}/my_file.zarr', fs=s3)
    root = zarr.open(store=store)
    s = time.time()
    for i in range(10):
        arr = root[f'{i}'][:]
    e = time.time()
    print(f"\tRead time {e-s:.2f}")

def main():
    # Amazon
    name='mast-test'
    url="https://s3-eu-west-2.amazonaws.com"

    print('Testing Amazon')
    write_data(name, key, secret, url)
    read_data(name, key, secret, url)

    # STFC Ceph
    print('Testing STFC Ceph')
    url = "https://s3.echo.stfc.ac.uk"
    name = "sciml-workshop-data"
    write_data(name, key, secret, url)
    read_data(name, key, secret, url)

    # CSD3 Ceph
    name='BUCKET'
    url="https://object.arcus.openstack.hpc.cam.ac.uk"

    print("Testing CSD3 Ceph")
    write_data(name, key, secret, url)
    read_data(name, key, secret, url)
   



if __name__ == "__main__":
    main()
