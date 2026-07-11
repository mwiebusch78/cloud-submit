import os
import json

import numpy as np
import polars as pl


def add_intercept(x):
    intercept = np.ones(len(x))
    return np.stack([intercept, x], axis=1)


def fit(data):
    x = data.get_column('x').to_numpy()
    y = data.get_column('y').to_numpy()

    # add intercept term
    x = add_intercept(x)

    # fit linear regression model
    coeffs = np.linalg.solve(np.dot(x.T, x), np.dot(x.T, y))

    return coeffs


def predict(data, coeffs):
    x = data.get_column('x').to_numpy()
    x = add_intercept(x)
    pred = np.dot(x, coeffs)
    return data.with_columns(pl.Series('pred', pred))


def fit_step(
    num_folds,
    fold_index,
    train_data_path,
    coefficients_path,
    predictions_path,
):
    print(f'Fitting model. Worker index: {fold_index}')

    # Do K-fold cross validation. Each worker does one fold.
    data = pl.read_parquet(train_data_path)
    splits = np.round(np.linspace(0, len(data), num_folds + 1))
    fold_start = int(splits[fold_index])
    fold_end = int(splits[fold_index + 1])

    train_data = pl.concat([data[:fold_start], data[fold_end:]])
    val_data = data[fold_start:fold_end]

    # Fit model
    coeffs = fit(train_data)

    # Predict on fold
    pred = predict(val_data, coeffs)

    # Create directory for coefficients and write coefficients for current fold.
    os.makedirs(coefficients_path, exist_ok=True)
    path = os.path.join(coefficients_path, f'fold{fold_index}.json')
    with open(path, 'w') as stream:
        json.dump(
            {'alpha': float(coeffs[0]), 'beta': float(coeffs[1])},
            stream,
        )

    # Create directory for predictions and write predictions for current fold.
    os.makedirs(predictions_path, exist_ok=True)
    path = os.path.join(predictions_path, f'fold{fold_index}.parquet')
    pred.write_parquet(path)

