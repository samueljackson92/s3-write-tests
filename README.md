# Test S3 Endpoints

### Usage

```bash
python test_endpoint.py ~/.s3cfg my-bucket results.csv
```

Where
 - `~/.s3cfg` is a config file in the format expected by `s3cmd`. It must have the following keys:
    - `host_base` e.g. `object.arcus.openstack.hpc.cam.ac.uk`
    - `access_key` - your access key
    - `secret_key` - yor secret key
 - `my-bucket` is the name of the bucket to read and write to. `my-bucket` will be mapped to `s3://my-bucket`
 - `results.csv` is the name of the output CSV file to write.