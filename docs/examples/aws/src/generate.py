import os

import numpy as np
import polars as pl


def generate_step(
    random_seed,
    num_samples,
    alpha,
    beta,
    sigma,
    train_data_path,
):
    print(f'Generating training data.')

    rng = np.random.default_rng(random_seed)
    x = rng.normal(size=num_samples)
    y = alpha + beta*x + sigma*rng.normal(size=num_samples)

    train_data = pl.DataFrame([
        pl.Series('x', x),
        pl.Series('y', y),
    ])

    os.mkdir(train_data_path)
    split_index = len(train_data)//2
    train_data[:split_index].write_parquet(
        os.path.join(train_data_path, 'part0.parquet'))
    train_data[split_index:].write_parquet(
        os.path.join(train_data_path, 'part1.parquet'))

