import argparse
import logging
import sys
import time

import numpy as np
import torch
import torch.nn as nn
import torch.nn.functional as F
import yaml

import src.utils.rainfall as rp
import train.pipeline as pipeline
from config.globals import MODELS_DIR
from train.binary_classifier import BinaryClassifier
from train.ordinal_classifier import OrdinalClassifier
from train.regression_net import Regressor
from train.training_utils import (
    DeviceDataLoader,
    gen_learning_curve,
    seed_everything,
    to_device,
)


def compute_weights_for_binary_classification(y):
    """
    TODO: really compute the weights!
    """
    print(y.shape)
    weights = np.zeros_like(y)

    mask_NORAIN = np.where(y == 0)[0]
    print(mask_NORAIN.shape)

    mask_RAIN = np.where(y > 0)[0]
    print(mask_RAIN.shape)

    weights[mask_NORAIN] = 1
    weights[mask_RAIN] = 80

    print(f"# NO_RAIN records: {len(mask_NORAIN)}")
    print(f"# RAIN records: {len(mask_RAIN)}")
    return weights


def compute_weights_for_ordinal_classification(y):
    weights = np.zeros_like(y)

    mask_weak = np.logical_and(y >= 0, y < 5)
    weights[mask_weak] = 1

    mask_moderate = np.logical_and(y >= 5, y < 25)
    weights[mask_moderate] = 5

    mask_strong = np.logical_and(y >= 25, y < 50)
    weights[mask_strong] = 25

    mask_extreme = np.logical_and(y >= 50, y < 150)
    weights[mask_extreme] = 50

    return weights


def weighted_mse_loss(input, target, weight):
    return (weight * (input - target) ** 2).sum()


class WeightedBCELoss(torch.nn.Module):
    def __init__(self, pos_weight=None, neg_weight=None):
        super(WeightedBCELoss, self).__init__()
        self.pos_weight = pos_weight
        self.neg_weight = neg_weight

    def forward(self, inputs, targets):
        inputs = torch.clamp(inputs, min=1e-7, max=1 - 1e-7)

        # Apply class weights to positive and negative examples
        if self.pos_weight is not None and self.neg_weight is not None:
            loss = self.neg_weight * (1 - targets) * torch.log(
                1 - inputs
            ) + self.pos_weight * targets * torch.log(inputs)
        elif self.pos_weight is not None:
            loss = self.pos_weight * targets * torch.log(inputs)
        elif self.neg_weight is not None:
            loss = self.neg_weight * (1 - targets) * torch.log(1 - inputs)
        else:
            loss = F.binary_cross_entropy(inputs, targets, reduction="mean")

        return -torch.mean(loss)


def train(
    forecaster,
    X_train,
    y_train,
    X_val,
    y_val,
    forecasting_task_sufix,
    pipeline_id,
    learner,
    config,
    resume_training: bool = False,
):
    NUM_FEATURES = X_train.shape[2]
    print(f"Number of features: {NUM_FEATURES}")

    # model.apply(initialize_weights)

    if forecasting_task_sufix == "oc":
        print("- Forecasting task: ordinal classification.")
        train_weights = compute_weights_for_ordinal_classification(y_train)
        val_weights = compute_weights_for_ordinal_classification(y_val)
        train_weights = torch.FloatTensor(train_weights)
        val_weights = torch.FloatTensor(val_weights)
        # loss = weighted_mse_loss
        loss = nn.MSELoss()
        y_train = rp.value_to_ordinal_encoding(y_train)
        y_val = rp.value_to_ordinal_encoding(y_val)
        target_average = torch.mean(
            torch.tensor(y_train, dtype=torch.float32), dim=0, keepdim=True
        )
    elif forecasting_task_sufix == "bc":
        print("- Forecasting task: binary classification.")
        train_weights = compute_weights_for_binary_classification(y_train)
        val_weights = compute_weights_for_binary_classification(y_val)
        train_weights = torch.FloatTensor(train_weights)
        val_weights = torch.FloatTensor(val_weights)
        loss = nn.BCELoss()
        # weights = torch.FloatTensor([1.0, 5.0])
        # loss = WeightedBCELoss(pos_weight=2, neg_weight=1)
        y_train = rp.value_to_binary_level(y_train)
        y_val = rp.value_to_binary_level(y_val)
    elif forecasting_task_sufix == "reg":
        print("- Forecasting task: regression.")
        loss = nn.MSELoss()
        global y_mean_value
        y_mean_value = np.mean(y_train)
        print(y_mean_value)

    print(forecaster)

    BATCH_SIZE = config["training"][forecasting_task_sufix]["BATCH_SIZE"]
    LEARNING_RATE = config["training"][forecasting_task_sufix]["LEARNING_RATE"]
    N_EPOCHS = config["training"][forecasting_task_sufix]["N_EPOCHS"]
    PATIENCE = config["training"][forecasting_task_sufix]["PATIENCE"]
    WEIGHT_DECAY = config["training"][forecasting_task_sufix]["WEIGHT_DECAY"]

    optimizer = torch.optim.Adam(
        forecaster.learner.parameters(), lr=LEARNING_RATE, weight_decay=WEIGHT_DECAY
    )
    print(f" - Setting up optimizer: {optimizer}")
    # optimizer = torch.optim.SGD(model.parameters(), lr=1e-5, momentum=0.9)

    print(" - Creating data loaders.")
    train_loader = learner.create_dataloader(
        X_train, y_train, batch_size=BATCH_SIZE, weights=train_weights
    )
    val_loader = learner.create_dataloader(
        X_val, y_val, batch_size=BATCH_SIZE, weights=val_weights
    )

    device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")
    print(f" - Moving data and parameters to {device}.")
    train_loader = DeviceDataLoader(train_loader, device)
    val_loader = DeviceDataLoader(val_loader, device)
    to_device(forecaster.learner, device)

    # resume_training = True
    # if resume_training:
    #     model_path = globals.MODELS_DIR + "best_" + pipeline_id + ".pt"  # Path to the pretrained model file
    #     model.load_state_dict(torch.load(model_path))

    print(" - Fitting model...", end=" ")
    train_loss, val_loss = forecaster.learner.fit(
        n_epochs=N_EPOCHS,
        optimizer=optimizer,
        train_loader=train_loader,
        val_loader=val_loader,
        patience=PATIENCE,
        criterion=loss,
        pipeline_id=pipeline_id,
    )
    print("Done!")

    gen_learning_curve(train_loss, val_loss, pipeline_id)

    #
    # Load the best model obtainined throughout the training epochs.
    #
    forecaster.learner.load_state_dict(
        torch.load(MODELS_DIR + "/best_" + pipeline_id + ".pt")
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

    # Instantiate the class
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
        forecaster = Regressor(in_channels=NUM_FEATURES, y_mean_value=y_mean_value)

    # Build model
    start_time = time.time()
    train(
        forecaster,
        X_train,
        y_train,
        X_val,
        y_val,
        prediction_task_sufix,
        args.pipeline_id,
        learner,
        config,
    )
    logging.info("Model training took %s seconds." % (time.time() - start_time))

    # Evaluate using the best model produced
    test_loader = learner.create_dataloader(X_test, y_test, batch_size=BATCH_SIZE)
    forecaster.print_evaluation_report(
        args.pipeline_id, test_loader, forecasting_task_id
    )


if __name__ == "__main__":
    start_time = time.time()
    main(sys.argv)
    end_time = time.time()
    execution_time = end_time - start_time
    print("The execution time was", execution_time, "seconds.")
