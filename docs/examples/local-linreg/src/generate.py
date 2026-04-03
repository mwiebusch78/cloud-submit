import numpy as np
import polars as pl


def generate_step(
    random_seed,
    num_rows,
    alpha,
    beta,
    sigma,
    train_data_path,
):
    rng = np.random.default_rng(random_seed)
    x = rng.normal(size=num_rows)
    y = alpha + beta*x + sigma*rng.normal(size=num_rows)

    train_data = pl.DataFrame([
        pl.Series('x', x),
        pl.Series('y', y),
    ])
    train_data.write_parquet(train_data_path)

