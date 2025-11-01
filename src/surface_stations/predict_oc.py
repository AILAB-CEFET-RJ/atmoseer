import pickle

import torch
import yaml

import config.globals as globals
from src.train.ordinal_classifier import OrdinalClassifier

if __name__ == "__main__":
    pipeline_id = "A652_N"

    #
    # Load numpy arrays (stored in a pickle file) from disk
    filename = globals.DATASETS_DIR + pipeline_id + ".pickle"
    file = open(filename, "rb")
    (_, _, _, _, X_test, _) = pickle.load(file)
    print(f"Shape of test data matrix: {X_test.shape}")

    # Example to predict is the firs one in the test dataset.
    x = X_test[0:1, :, :]
    print(f"Shape of one test example: {x.shape}")
    print("Model input:")
    print(x)

    NUM_FEATURES = X_test.shape[2]
    NUM_CLASSES = 5

    with open("./config/config.yaml", "r") as file:
        config = yaml.safe_load(file)

    # Create an instance of the model
    input_dim = (NUM_FEATURES, config["preproc"]["SLIDING_WINDOW_SIZE"])
    model = OrdinalClassifier(
        in_channels=NUM_FEATURES,
        num_classes=NUM_CLASSES,
        input_dim=input_dim,
        target_average=None,
    )

    # Load the pretrained model weights from the file
    model_path = (
        globals.MODELS_DIR + "best_" + pipeline_id + "_OC.pt"
    )  # Path to the pretrained model file
    model.load_state_dict(torch.load(model_path, map_location=torch.device("cpu")))

    # Set the model in evaluation (inference) mode.
    model.eval()

    # Make prediction using the loaded model
    with torch.no_grad():
        output = model.predict(x)
        print(f"Predicted level: {output[0][0]}")
