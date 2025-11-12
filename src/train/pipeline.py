import pickle
import logging
from config import globals

def load_datasets(pipeline_id: str):
    '''
    Load numpy arrays (stored in a pickle file) from disk
    '''
    filename = globals.DATASETS_DIR + pipeline_id + ".pickle"
    logging.info(f"Loading train/val/test datasets from {filename}.")
    file = open(filename, 'rb')
    (X_train, y_train, X_val, y_val, X_test, y_test) = pickle.load(file)
    logging.info(f"Shapes of train/val/test data matrices: {X_train.shape}/{X_val.shape}/{X_test.shape}")
    logging.info(f"Min values of train/val/test data matrices: {min(X_train.reshape(-1,1))}/{min(X_val.reshape(-1,1))}/{min(X_test.reshape(-1,1))}")
    logging.info(f"Max values of train/val/test data matrices: {max(X_train.reshape(-1,1))}/{max(X_val.reshape(-1,1))}/{max(X_test.reshape(-1,1))}")
    logging.info(f"Min values of train/val/test target: {min(y_train)}/{min(y_val)}/{min(y_test)}")
    logging.info(f"Max values of train/val/test target: {max(y_train)}/{max(y_val)}/{max(y_test)}")

    return X_train, y_train, X_val, y_val, X_test, y_test
