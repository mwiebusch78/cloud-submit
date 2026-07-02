import os
import json

import numpy as np
import polars as pl


def fit_step(
    train_data_path,
    coefficients_path,
    predictions_path,
):
    print(f'Fitting model.')

    train_data = pl.read_parquet(os.path.join(train_data_path, '*.parquet'))
    x = train_data.get_column('x').to_numpy()
    y = train_data.get_column('y').to_numpy()

    # add intercept term
    intercept = np.ones(len(x))
    x = np.stack([intercept, x], axis=1)

    # fit linear regression model
    coeffs = np.linalg.solve(np.dot(x.T, x), np.dot(x.T, y))

    # compute in-sample predictions
    preds = np.dot(x, coeffs)
    preds = train_data.with_columns(pl.Series('pred', preds))

    # save coefficients
    with open(coefficients_path, 'w') as stream:
        json.dump(
            {
                'alpha': float(coeffs[0]),
                'beta': float(coeffs[1]),
            },
            stream,
        )

    # save in-sample predictions
    preds.write_parquet(predictions_path)

