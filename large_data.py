
from multiprocessing import Pool
import time

from sqlalchemy import create_engine
import pandas as pd


def counter_time(func):
    def wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        print(f"took {time.perf_counter() - start_time:.2f} seconds")
        return result

    return wrapper


@counter_time
def create_parquet_files_from_storage():
    LIMIT = 1_000_000
    OFFSET = [0, 1_000_000, 2_000_000, 3_000_000, 4_000_000, 5_000_000, 6_000_000, 7_000_000, 8_000_000, 9_000_000]
    engine = create_engine("sqlite:///[YOUR-DB].sqlite")
    for i, offset in enumerate(OFFSET):
        query = f"SELECT * FROM collisions LIMIT {LIMIT} OFFSET {offset}"
        df = pd.read_sql(query, engine)
        df.to_parquet(f"./storage/cached_{i}.parquet")


def execute_read_storage(file):
    df = pd.read_parquet(file)
    print(df.max)


if __name__ == "__main__":
    # Create Cache
    # create_parquet_files_from_storage()

    # Read From Storage
    # execute_read_storage()
    files = [f"cached_{i}.parquet" for i in range(0, 10)]
    with Pool(4) as p:
        p.map(execute_read_storage, files)
