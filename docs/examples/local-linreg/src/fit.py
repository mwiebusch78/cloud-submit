import json

import numpy as np
import polars as pl


def fit_step(
    train_data_path,
    parameters_path,
):
    train_data = pl.read_parquet(train_data_path)
    x = train_data.get_column('x').to_numpy()
    y = train_data.get_column('y').to_numpy()

    # add intercept term
    intercept = np.ones(len(x))
    x = np.stack([intercept, x], axis=1)

    # fit linear regression model
    coeffs = np.linalg.solve(np.dot(x.T, x), np.dot(x.T, y))

    with open(parameters_path, 'w') as stream:
        json.dump(
            {'alpha': float(coeffs[0]), 'beta': float(coeffs[1])},
            stream,
        )

