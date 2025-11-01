import numpy as np
import torch

from train.base_classifier import BaseClassifier
from train.training_utils import DeviceDataLoader
from utils import rainfall as rp


class BinaryClassifier(BaseClassifier):
    def __init__(self, learner):
        super(BinaryClassifier, self).__init__()
        self.learner = learner

    def evaluate(self, test_loader):
        print("Evaluating binary classifier...")
        self.learner.eval()

        device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")
        test_loader = DeviceDataLoader(test_loader, device)

        y_pred = None
        with torch.no_grad():
            for xb_test, yb_test in test_loader:
                yb_pred = self.learner(xb_test.float())

                yb_pred = yb_pred.detach().cpu().numpy()
                yb_pred = yb_pred.reshape(-1, 1)

                yb_test = yb_test.detach().cpu().numpy()
                yb_test = yb_test.reshape(-1, 1)

                if y_pred is None:
                    y_pred = yb_pred
                    y_true = yb_test
                else:
                    y_pred = np.vstack([y_pred, yb_pred])
                    y_true = np.vstack([y_true, yb_test])

        y_pred = y_pred.round().ravel()
        assert np.all(np.logical_or(y_pred == 0, y_pred == 1))

        y_true = rp.value_to_binary_level(y_true)
        assert np.all(np.logical_or(y_true == 0, y_true == 1))
        y_true = y_true.ravel()

        return y_true, y_pred
