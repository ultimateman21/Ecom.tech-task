from random import seed, randint, choice, choices
from decimal import Decimal
import pandas as pd
import numpy as np


DEFAULT_SEED = 42

NUM_USERS = 100
NUM_STORES = 20
NUM_ORDERS = 1000
AMOUNT_MIN = 5000
AMOUNT_MAX = 500000

# по москве
DATE_START = '2023-01-01'
DATE_END = '2025-12-31'
START_TS = pd.Timestamp(DATE_START, tz='Europe/Moscow').tz_convert('UTC').tz_localize(None)
END_TS = pd.Timestamp(DATE_END, tz='Europe/Moscow').tz_convert('UTC').tz_localize(None)

CITIES = ['Moscow', 'SPB', 'Kazan', 'Novosibirsk']

ORDER_STATUSES = ['created', 'paid', 'cancelled']
STATUS_WEIGHTS = [0.2, 0.6, 0.2]

S3_OPTIONS = {'key': 'minio',
              'secret': '00000000',
              'client_kwargs': {'endpoint_url': 'http://minio:9000'}}


def random_timestamp():
    delta_seconds = int((END_TS - START_TS).total_seconds())
    return START_TS + pd.to_timedelta(randint(0, delta_seconds), unit='s')


def generate_users():
    data = [(user_id, f'user_{user_id}', f'+{user_id:011d}', random_timestamp()) for user_id in range(1, NUM_USERS + 1)]
    columns = ['id', 'name', 'phone', 'created_at']
    types = ['int32', 'string', 'string', 'datetime64[ms]']
    return pd.DataFrame(data, columns=columns).astype(dict(zip(columns, types)))


def generate_stores():
    data = [(store_id, f'store_{store_id}', choice(CITIES)) for store_id in range(1, NUM_STORES + 1)]
    columns = ['id', 'name', 'city']
    types = ['int32', 'string', 'string']
    return pd.DataFrame(data, columns=columns).astype(dict(zip(columns, types)))


def generate_orders(users: pd.DataFrame, stores: pd.DataFrame):
    data = [(order_id, Decimal(randint(AMOUNT_MIN, AMOUNT_MAX)).scaleb(-randint(0, 2)), int(users.sample(1)['id'].iloc[0]),
             int(stores.sample(1)['id'].iloc[0]), choices(ORDER_STATUSES, weights=STATUS_WEIGHTS, k=1)[0], random_timestamp()) for order_id in range(1, NUM_ORDERS + 1)]
    columns = ['id', 'amount', 'user_id', 'store_id', 'status', 'created_at']
    types = ['int32', 'object', 'int32', 'int32', 'string', 'datetime64[ms]']
    return pd.DataFrame(data, columns=columns).astype(dict(zip(columns, types)))


def print_dataset(name: str, df: pd.DataFrame, rows: int = 5):
    print('=' * 80)
    print(f'{name.upper()}')
    print(f'Shape: {df.shape}')
    print('\nDtypes:')
    print(df.dtypes)
    print(f'\nPreview (first {rows} rows):')
    print(df.head(rows))
    print('=' * 80)
    print()


def ask_seed():
    value = input(
        f"Enter random seed (empty = default {DEFAULT_SEED}): "
    ).strip()

    if value == "":
        print(f"[generator] Using default seed = {DEFAULT_SEED}")
        return DEFAULT_SEED
    elif not value.isdigit():
        print("Seed must be an integer")
        return ask_seed()

    seed = int(value)
    print(f"[generator] Using seed = {seed}")
    return seed


def main():
    seed_ = ask_seed()
    seed(seed_)
    np.random.seed(seed_)
    
    users = generate_users()
    stores = generate_stores()
    orders = generate_orders(users, stores)

    users.to_parquet('s3://input/users.parquet', engine='pyarrow', storage_options=S3_OPTIONS)
    stores.to_parquet('s3://input/stores.parquet', engine='pyarrow', storage_options=S3_OPTIONS)
    orders.to_parquet('s3://input/orders.parquet', engine='pyarrow', storage_options=S3_OPTIONS)

    print('\nDatasets generated and uploaded to MinIO\n')

    print_dataset('users', users)
    print_dataset('stores', stores)
    print_dataset('orders', orders)


if __name__ == "__main__":
    main()
