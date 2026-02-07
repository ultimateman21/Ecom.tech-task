import pandas as pd


S3_OPTIONS = {'key': 'minio',
              'secret': '00000000',
              'client_kwargs': {'endpoint_url': 'http://minio:9000'}}


def main():
    df = pd.read_parquet('s3://output/top_stores.parquet', engine='pyarrow', storage_options=S3_OPTIONS)
    print(f'{df}\n\nData types:\n{df.dtypes}')


if __name__ == "__main__":
    main()
