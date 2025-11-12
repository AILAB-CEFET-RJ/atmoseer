
import pandas as pd
import numpy as np

def apply_windowing(X,
                    initial_time_step,
                    max_time_step,
                    window_size,
                    target_idx):

    assert target_idx >= 0 and target_idx < X.shape[1]
    assert initial_time_step >= 0
    assert max_time_step >= initial_time_step

    start = initial_time_step

    sub_windows = (
        start +
        # expand_dims converts a 1D array to 2D array.
        np.expand_dims(np.arange(window_size), 0) +
        np.expand_dims(np.arange(max_time_step + 1), 0).T
    )

    X_temp, y_temp = X[sub_windows], X[window_size:(
        max_time_step+window_size+1):1, target_idx]

    idx_y_train_not_nan = np.where(~np.isnan(y_temp))[0]
    assert len(idx_y_train_not_nan) == len(y_temp)

    x_train_is_nan_idx = np.unique(np.where(np.isnan(X_temp)))
    # assert len(x_train_is_nan_idx) == len(y_temp)

    # print(f'X.shape: {X.shape}')
    # print(f'Shapes before filtering: {X_temp.shape}, {y_temp.shape}')

    idx_y_train_gt_zero = np.where(y_temp > 0)[0]
    
    # if only_y_gt_zero:
    #     y_train_gt_zero_idx = np.where(y_temp > 0)[0]
    #     idxs = np.intersect1d(y_train_not_nan_idx, y_train_gt_zero_idx)
    #     idxs = np.setdiff1d(idxs, x_train_is_nan_idx)
    #     X_temp, y_temp = X_temp[idxs], y_temp[idxs]

    # print(f'y_train_not_nan_idx.shape: {idx_y_train_not_nan.shape}')
    # print(f'y_train_gt_zero_idx.shape: {idx_y_train_gt_zero.shape}')
    # print(f'x_train_is_nan_idx.shape: {idx_y_train_not_nan.shape}')
    # print(f'Shapes after filtering: {X_temp.shape}, {y_temp.shape}')
    # print()

    return X_temp, y_temp
