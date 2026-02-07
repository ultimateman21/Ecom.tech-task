import pandas as pd
from os.path import basename, join, exists


DATASETS_DIR = 'data'
S3_OPTIONS = {'key': 'minio',
              'secret': '00000000',
              'client_kwargs': {'endpoint_url': 'http://minio:9000'}}

FILES = {'users.parquet': 's3://input/users.parquet',
         'stores.parquet': 's3://input/stores.parquet',
         'orders.parquet': 's3://input/orders.parquet'}


def upload_file(local_path: str, s3_path: str):
    df = pd.read_parquet(local_path)
    df.to_parquet(s3_path, engine='pyarrow', storage_options=S3_OPTIONS)
    print(f'Uploaded {basename(local_path)} → {s3_path}')


def main():
    for filename, s3_path in FILES.items():
        local_file = join(DATASETS_DIR, filename)

        if not exists(local_file):
            raise FileNotFoundError(f'Dataset not found: {local_file}')

        upload_file(local_file, s3_path)

    print('All datasets successfully uploaded to MinIO')


if __name__ == "__main__":
    main()
