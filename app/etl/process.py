import pandas as pd

S3_OPTIONS = {'key': 'minio',
              'secret': '00000000',
              'client_kwargs': {'endpoint_url': 'http://minio:9000'}}


def extract():
    users = pd.read_parquet('s3://input/users.parquet', engine='pyarrow', storage_options=S3_OPTIONS)
    stores = pd.read_parquet('s3://input/stores.parquet', engine='pyarrow', storage_options=S3_OPTIONS)
    orders = pd.read_parquet('s3://input/orders.parquet', engine='pyarrow', storage_options=S3_OPTIONS)
    return users, stores, orders


def transform(users: pd.DataFrame, stores: pd.DataFrame, orders: pd.DataFrame):
    # Только пользователи 2025 года
    users['created_at'] = pd.to_datetime(users['created_at'])
    users_2025 = users[users['created_at'].dt.year == 2025]

    # Только оплаченные заказы
    orders_paid = orders[orders['status'] == 'paid']

    # Оставляем заказы только этих пользователей
    orders_filtered = orders_paid.merge(users_2025[['id']], left_on='user_id', right_on='id', how='inner')

    # 4. Присоединяем магазины
    orders_with_stores = orders_filtered.merge(stores, left_on='store_id', right_on='id', how='left', suffixes=('_order', '_store'))

    # 5. Агрегация
    agg = orders_with_stores.groupby(['city', 'name'], as_index=False).agg(total_amount=('amount', 'sum'))

    # 6. Топ-3 магазинов по каждому городу
    top_stores = agg.sort_values(['city', 'total_amount'], ascending=[True, False]).groupby('city').head(3).reset_index(drop=True)
    return top_stores


def load(df: pd.DataFrame):
    df.to_parquet('s3://output/top_stores.parquet', engine='pyarrow', storage_options=S3_OPTIONS)


def main():
    users, stores, orders = extract()
    result = transform(users, stores, orders)
    result.to_parquet('s3://output/top_stores.parquet', engine='pyarrow', storage_options=S3_OPTIONS)
    print('ETL pipeline finished successfully')


if __name__ == "__main__":
    main()
