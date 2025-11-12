"""
    https://stats.stackexchange.com/questions/209290/deep-learning-for-ordinal-classification

    https://towardsdatascience.com/simple-trick-to-train-an-ordinal-regression-with-any-classifier-6911183d2a3c

    https://towardsdatascience.com/how-to-perform-ordinal-regression-classification-in-pytorch-361a2a095a99

    https://arxiv.org/pdf/0704.1028.pdf

    https://datascience.stackexchange.com/questions/44354/ordinal-classification-with-xgboost

    https://towardsdatascience.com/building-rnn-lstm-and-gru-for-time-series-using-pytorch-a46e5b094e7b

    https://neptune.ai/blog/how-to-deal-with-imbalanced-classification-and-regression-data

    https://colab.research.google.com/github/YyzHarry/imbalanced-regression/blob/master/tutorial/tutorial.ipynb#scrollTo=tSrzhog1gxyY
"""

from train.base_classifier import BaseClassifier
import torch
import torch.nn as nn
import torch.nn.functional as F
from train.training_utils import *
from train.evaluate import *
from src.utils.rainfall import ordinal_encoding_to_level
from train.early_stopping import *
from utils import rainfall

class OrdinalClassifier(BaseClassifier):
    def __init__(self, learner):
        super(OrdinalClassifier, self).__init__()
        self.learner = learner

        # self.feature_extractor = nn.Sequential(
        #     nn.Conv1d(in_channels=in_channels, out_channels=16, kernel_size=3, padding=3),
        #     nn.ReLU(inplace=True),
        #     nn.Dropout1d(p=dropout_rate))

        # # https://datascience.stackexchange.com/questions/40906/determining-size-of-fc-layer-after-conv-layer-in-pytorch
        # num_features_before_fcnn = functools.reduce(operator.mul, list(self.feature_extractor(torch.rand(1, *input_dim)).shape))

        # self.classifier = nn.Sequential(
        #     nn.Linear(in_features=num_features_before_fcnn, out_features=50),
        #     nn.ReLU(inplace=True),
        #     nn.Linear(50, num_classes),
        #     nn.Sigmoid()
        # )

        # # Initialize the bias of the last layer with the average target value
        # # see https://discuss.pytorch.org/t/how-to-initialize-weight-with-arbitrary-tensor/3432
        # if target_average is not None:
        #     # Get the last layer
        #     last_layer = self.classifier[-2]
        #     print(f"last_layer.bias = {last_layer.bias}")
        #     print(f"target_average = {target_average[0]}")
        #     last_layer.bias = torch.nn.Parameter(target_average[0])
        #     print(f"last_layer.bias = {last_layer.bias}")

    # def forward(self, x):
    #     out = self.feature_extractor(x)
    #     out = out.view(out.shape[0], -1)
    #     out = self.classifier(out)
    #     return out

    # def training_step(self, batch):
    #     X_train, y_train = batch
    #     out = self(X_train)                  # Generate predictions
    #     loss = F.cross_entropy(out, y_train)  # Calculate loss
    #     return loss

    # def validation_step(self, batch):
    #     X_train, y_train = batch
    #     out = self(X_train)                    # Generate predictions
    #     loss = F.cross_entropy(out, y_train)   # Calculate loss
    #     acc = accuracy(out, y_train)           # Calculate accuracy
    #     return {'val_loss': loss, 'val_acc': acc}

    # def validation_epoch_end(self, outputs):
    #     batch_losses = [x['val_loss'] for x in outputs]
    #     epoch_loss = torch.stack(batch_losses).mean()   # Combine losses
    #     batch_accs = [x['val_acc'] for x in outputs]
    #     epoch_acc = torch.stack(batch_accs).mean()      # Combine accuracies
    #     return {'val_loss': epoch_loss.item(), 'val_acc': epoch_acc.item()}

    # def epoch_end(self, epoch, result):
    #     print("Epoch [{}], val_loss: {:.4f}, val_acc: {:.4f}".format(
    #         epoch, result['val_loss'], result['val_acc']))

    def predict(self, X):
        print('Making predictions with ordinal classification model...')
        self.eval()

        X_as_tensor = torch.from_numpy(X.astype('float64'))
        X_as_tensor = torch.permute(X_as_tensor, (0, 2, 1))

        outputs = []
        with torch.no_grad():
            output = self.learner(X_as_tensor.float())
            yb_pred_encoded = output.detach().cpu().numpy()
            yb_pred_decoded = ordinal_encoding_to_level(yb_pred_encoded)
            outputs.append(yb_pred_decoded.reshape(-1, 1))

        y_pred = np.vstack(outputs)

        return y_pred

    def evaluate(self, test_loader):
        print('Evaluating ordinal classifier...')
        self.learner.eval()

        device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")
        test_loader = DeviceDataLoader(test_loader, device)

        y_pred = None
        with torch.no_grad():
            for xb_test, yb_test in test_loader:
                yb_pred = self.learner(xb_test.float())

                yb_pred = yb_pred.detach().cpu().numpy()
                yb_pred = ordinal_encoding_to_level(yb_pred)
                yb_pred = yb_pred.reshape(-1,1)

                yb_test = yb_test.detach().cpu().numpy()
                yb_test = yb_test.reshape(-1,1)

                if y_pred is None:
                    y_pred = yb_pred
                    y_true = yb_test
                else:
                    y_pred = np.vstack([y_pred, yb_pred])
                    y_true = np.vstack([y_true, yb_test])

        y_true = rp.value_to_level(y_true)
        print(f'Shapes: {y_true.shape}, {y_pred.shape}')

        return y_true, y_pred

    # def fit(self, n_epochs, optimizer, train_loader, val_loader, patience, criterion, pipeline_id):
    #     # to track the training loss as the model trains
    #     train_losses = []
    #     # to track the validation loss as the model trains
    #     valid_losses = []
    #     # to track the average training loss per epoch as the model trains
    #     avg_train_losses = []
    #     # to track the average validation loss per epoch as the model trains
    #     avg_valid_losses = []

    #     # initialize the early_stopping object
    #     early_stopping = EarlyStopping(patience=patience, verbose=True)

    #     for epoch in range(n_epochs):

    #         ###################
    #         # train the model #
    #         ###################
    #         self.train()  # prep model for training
    #         for data, target, w in train_loader:
    #             # clear the gradients of all optimized variables
    #             optimizer.zero_grad()

    #             # forward pass: compute predicted outputs by passing inputs to the model
    #             output = self(data.float())

    #             # calculate the loss
    #             loss = criterion(output, target.float(), w)
    #             assert not (np.isnan(loss.item()) or loss.item() >
    #                         1e6), f"Loss explosion: {loss.item()}"

    #             loss.backward()

    #             # perform a single optimization step (parameter update)
    #             optimizer.step()

    #             # record training loss
    #             train_losses.append(loss.item())

    #         ######################
    #         # validate the model #
    #         ######################
    #         self.eval()  # prep model for evaluation
    #         for data, target, w in val_loader:
    #             # forward pass: compute predicted outputs by passing inputs to the model
    #             output = self(data.float())
    #             # calculate the loss
    #             loss = criterion(output, target.float(), w)
    #             # record validation loss
    #             valid_losses.append(loss.item())

    #         # print training/validation statistics
    #         # calculate average loss over an epoch
    #         train_loss = np.average(train_losses)
    #         valid_loss = np.average(valid_losses)
    #         avg_train_losses.append(train_loss)
    #         avg_valid_losses.append(valid_loss)

    #         epoch_len = len(str(n_epochs))

    #         print_msg = (f'[{(epoch+1):>{epoch_len}}/{n_epochs:>{epoch_len}}] ' +
    #                      f'train_loss: {train_loss:.5f} ' +
    #                      f'valid_loss: {valid_loss:.5f}')

    #         print(print_msg)

    #         # clear lists to track next epoch
    #         train_losses = []
    #         valid_losses = []

    #         early_stopping(valid_loss, self, pipeline_id)

    #         if early_stopping.early_stop:
    #             print("Early stopping activated!")
    #             break

    #     return avg_train_losses, avg_valid_losses
