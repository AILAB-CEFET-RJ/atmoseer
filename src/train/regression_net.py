import torch
import torch.nn as nn
from torch.utils.data import TensorDataset
import torch.nn.functional as F
from train.training_utils import *
from train.evaluate import *

class Regressor(nn.Module):
    def __init__(self, in_channels, y_mean_value):
        super(Regressor, self).__init__()

        self.conv1d_1 = nn.Conv1d(
            in_channels=in_channels, out_channels=32, kernel_size=3, padding=2)
        self.gn_1 = nn.GroupNorm(1, 32)

        self.conv1d_2 = nn.Conv1d(
            in_channels=32, out_channels=64, kernel_size=3, padding=2)
        self.gn_2 = nn.GroupNorm(1, 64)

        self.conv1d_3 = nn.Conv1d(
            in_channels=64, out_channels=64, kernel_size=3, padding=2)
        self.gn_3 = nn.GroupNorm(1, 64)

        self.conv1d_4 = nn.Conv1d(
            in_channels=64, out_channels=128, kernel_size=3, padding=2)
        self.gn_4 = nn.GroupNorm(1, 128)

        # self.conv1d_5 = nn.Conv1d(in_channels = 64, out_channels = 64, kernel_size = 3, padding=2)
        # self.conv1d_6 = nn.Conv1d(in_channels = 64, out_channels = 128, kernel_size = 3, padding=2)
        # self.conv1d_7 = nn.Conv1d(in_channels = 128, out_channels = 128, kernel_size = 3, padding=2)
        # self.conv1d_8 = nn.Conv1d(in_channels = 128, out_channels = 256, kernel_size = 3, padding=2)

        self.max_pooling1d_1 = nn.MaxPool1d(2)
        # self.max_pooling1d_2 = nn.MaxPool1d(2)

        # self.relu = nn.ReLU()
        self.relu = nn.GELU()

        self.fc1 = nn.Linear(1280, 50)

        self.fc2 = nn.Linear(50, 1)
        self.fc2.bias.data.fill_(y_mean_value)

        # self.dropout = nn.Dropout(0.25)

    def forward(self, x):
        x = self.conv1d_1(x)
        x = self.gn_1(x)
        x = self.relu(x)

        # print('conv1d_1')

        x = self.max_pooling1d_1(x)

        x = self.conv1d_2(x)
        x = self.gn_2(x)
        x = self.relu(x)

        # print('conv1d_2')

        x = self.conv1d_3(x)
        x = self.gn_3(x)
        x = self.relu(x)

        # print('conv1d_3')

        x = self.conv1d_4(x)
        x = self.gn_4(x)
        x = self.relu(x)

        # print('conv1d_4')

        # x = self.conv1d_5(x)
        # x = self.relu(x)

        # # print('conv1d_5')

        # x = self.max_pooling1d_1(x)

        # x = self.conv1d_6(x)
        # x = self.relu(x)

        # x = self.conv1d_7(x)
        # x = self.relu(x)

        # x = self.conv1d_8(x)
        # x = self.relu(x)

        # # print('conv1d_8')

        x = x.view(x.shape[0], -1)
        # x = self.dropout(x)

        x = self.fc1(x)
        x = self.relu(x)
        # x = self.dropout(x)

        x = self.fc2(x)
        x = self.relu(x)

        return x

    def validation_step(self, batch):
        X_train, y_train = batch
        out = self(X_train)                    # Generate predictions
        loss = F.cross_entropy(out, y_train)   # Calculate loss
        acc = accuracy(out, y_train)           # Calculate accuracy
        return {'val_loss': loss, 'val_acc': acc}

    def validation_epoch_end(self, outputs):
        batch_losses = [x['val_loss'] for x in outputs]
        epoch_loss = torch.stack(batch_losses).mean()   # Combine losses
        batch_accs = [x['val_acc'] for x in outputs]
        epoch_acc = torch.stack(batch_accs).mean()      # Combine accuracies
        return {'val_loss': epoch_loss.item(), 'val_acc': epoch_acc.item()}

    def evaluate(self, X_test, y_test):
        self.eval()

        test_x_tensor = torch.from_numpy(X_test.astype('float64'))
        test_x_tensor = torch.permute(test_x_tensor, (0, 2, 1))
        test_y_tensor = torch.from_numpy(y_test.astype('float64'))

        test_ds = TensorDataset(test_x_tensor, test_y_tensor)
        test_loader = torch.utils.data.DataLoader(
            test_ds, batch_size=32, shuffle=False)
        test_loader = DeviceDataLoader(test_loader, get_default_device())

        test_losses = []
        outputs = []
        with torch.no_grad():
            for xb, yb in test_loader:
                output = self(xb.float())
                outputs.append(output)

        y_pred = torch.vstack(outputs).squeeze(1)
        y_pred = y_pred.cpu().numpy().reshape(-1, 1)
        test_error = skl.mean_squared_error(y_test, y_pred)
        print('MSE on the entire test set: %f' % test_error)
        test_error2 = skl.mean_absolute_error(y_test, y_pred)
        print('MAE on the entire test set: %f' % test_error2)
        test_error3 = mean_bias_error(y_test, y_pred)
        print('MBE on the entire test set: %f' % test_error3)

        export_results_to_latex(y_test, y_pred)
