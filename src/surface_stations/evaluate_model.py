import argparse
import logging
import sys
import time

import torch
import yaml

import src.utils.rainfall as rp
import train.pipeline as pipeline
from config import globals
from train.binary_classifier import BinaryClassifier
from train.ordinal_classifier import OrdinalClassifier
from train.regression_net import Regressor
from train.training_utils import (
    DeviceDataLoader,
    seed_everything,
    to_device,
)


def main(argv):
    parser = argparse.ArgumentParser(description="Train a rainfall forecasting model.")
    parser.add_argument(
        "-t",
        "--task",
        choices=["ORDINAL_CLASSIFICATION", "BINARY_CLASSIFICATION"],
        default="REGRESSION",
        help="Prediction task",
    )
    parser.add_argument(
        "-l",
        "--learner",
        choices=["Conv1DNeuralNet", "LstmNeuralNet"],
        default="LstmNeuralNet",
        help="Learning algorithm to be used.",
    )
    parser.add_argument("-p", "--pipeline_id", required=True, help="Pipeline ID")

    args = parser.parse_args(argv[1:])

    forecasting_task_id = None

    forecasting_task_id = None

    if args.task == "ORDINAL_CLASSIFICATION":
        forecasting_task_id = rp.ForecastingTask.ORDINAL_CLASSIFICATION
    elif args.task == "BINARY_CLASSIFICATION":
        forecasting_task_id = rp.ForecastingTask.BINARY_CLASSIFICATION

    seed_everything()

    X_train, y_train, X_val, y_val, X_test, y_test = pipeline.load_datasets(
        args.pipeline_id
    )

    with open("./config/config.yaml", "r") as file:
        config = yaml.safe_load(file)
    SEQ_LENGTH = config["preproc"]["SLIDING_WINDOW_SIZE"]

    if forecasting_task_id == rp.ForecastingTask.ORDINAL_CLASSIFICATION:
        prediction_task_sufix = "oc"
        args.pipeline_id += "_" + prediction_task_sufix
    elif forecasting_task_id == rp.ForecastingTask.BINARY_CLASSIFICATION:
        prediction_task_sufix = "bc"
        args.pipeline_id += "_" + prediction_task_sufix
    elif forecasting_task_id == rp.ForecastingTask.REGRESSION:
        args.pipeline_id += "_reg"

    BATCH_SIZE = config["training"][prediction_task_sufix]["BATCH_SIZE"]
    DROPOUT_RATE = config["training"][prediction_task_sufix]["DROPOUT_RATE"]
    OUTPUT_SIZE = config["training"][prediction_task_sufix]["OUTPUT_SIZE"]

    # Use globals() to access the global namespace and find the class by name
    class_name = args.learner
    print(class_name)
    class_obj = globals()[class_name]

    # Check if the class exists
    if not isinstance(class_obj, type):
        raise ValueError(f"Class '{class_name}' not found.")

    args.pipeline_id += "_" + class_name

    NUM_FEATURES = X_train.shape[2]
    print(f"Number of features: {NUM_FEATURES}")

    # Instantiate the learner class
    learner = class_obj(
        seq_length=SEQ_LENGTH,
        input_size=NUM_FEATURES,
        output_size=OUTPUT_SIZE,
        dropout_rate=DROPOUT_RATE,
    )
    print(f"Learner: {learner}")

    if prediction_task_sufix == "oc":
        forecaster = OrdinalClassifier(learner)
    elif prediction_task_sufix == "bc":
        forecaster = BinaryClassifier(learner)
    elif prediction_task_sufix == "reg":
        forecaster = Regressor(in_channels=NUM_FEATURES)#, y_mean_value=y_mean_value)

    #
    # Load model.
    #
    filename = globals.MODELS_DIR + "/best_" + args.pipeline_id + ".pt"
    logging.info(f"Loading model from file {filename}")
    forecaster.learner.load_state_dict(torch.load(filename))

    # Evaluate model
    device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")
    logging.info(f"Moving data and parameters to {device}.")
    test_loader = learner.create_dataloader(X_test, y_test, batch_size=BATCH_SIZE)
    test_loader = DeviceDataLoader(test_loader, device)
    to_device(forecaster.learner, device)
    forecaster.print_evaluation_report(
        args.pipeline_id, test_loader, forecasting_task_id
    )


if __name__ == "__main__":
    start_time = time.time()
    main(sys.argv)
    end_time = time.time()
    execution_time = end_time - start_time
    print("The execution time was", execution_time, "seconds.")
