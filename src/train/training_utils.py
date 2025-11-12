import torch
import torch.nn as nn
from torch.utils.data import TensorDataset
import numpy as np
import os
import random
import matplotlib.pyplot as plt
from config import globals

def initialize_weights(m):
    if isinstance(m, nn.Conv1d):
        nn.init.kaiming_uniform_(m.weight.data, nonlinearity='relu')
        if m.bias is not None:
            nn.init.constant_(m.bias.data, 0)
    elif isinstance(m, nn.BatchNorm2d):
        nn.init.constant_(m.weight.data, 1)
        nn.init.constant_(m.bias.data, 0)
    elif isinstance(m, nn.Linear):
        nn.init.kaiming_uniform_(m.weight.data)
        nn.init.constant_(m.bias.data, 0)

def seed_everything(seed=1234):
    random.seed(seed)
    os.environ['PYTHONHASHSEED'] = str(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    torch.cuda.manual_seed(seed)
    torch.backends.cudnn.deterministic = True


def get_default_device():
    """Pick GPU if available, else CPU"""
    if torch.cuda.is_available():
        return torch.device('cuda')
    else:
        return torch.device('cpu')


def to_device(data, device):
    """Move tensor(s) to chosen device"""
    if isinstance(data, (list, tuple)):
        return [to_device(x, device) for x in data]
    return data.to(device, non_blocking=True)


class DeviceDataLoader():
    """Wrap a dataloader to move data to a device"""

    def __init__(self, dl, device):
        self.dl = dl
        self.device = device

    def __iter__(self):
        """Yield a batch of data after moving it to device"""
        for b in self.dl:
            yield to_device(b, self.device)

    def __len__(self):
        """Number of batches"""
        return len(self.dl)


def DEPRECATED_create_train_and_val_loaders(X_train, y_train, X_val, y_val, batch_size, train_weights, val_weights):
    X_train = torch.from_numpy(X_train.astype('float64'))
    X_train = torch.permute(X_train, (0, 2, 1))
    y_train = torch.from_numpy(y_train.astype('float64'))

    X_val = torch.from_numpy(X_val.astype('float64'))
    X_val = torch.permute(X_val, (0, 2, 1))
    y_val = torch.from_numpy(y_val.astype('float64'))

    if (train_weights is None) and (val_weights is None):
        train_ds = TensorDataset(X_train, y_train)
        val_ds = TensorDataset(X_val, y_val)
    else:
        train_ds = TensorDataset(X_train, y_train, train_weights)
        val_ds = TensorDataset(X_val, y_val, val_weights)

    train_loader = torch.utils.data.DataLoader(
        train_ds, batch_size=batch_size, shuffle=True)
    val_loader = torch.utils.data.DataLoader(
        val_ds, batch_size=batch_size, shuffle=True)

    return train_loader, val_loader

def gen_learning_curve(train_loss, val_loss, pipeline_id):
    fig = plt.figure(figsize=(10, 8))
    plt.plot(range(1, len(train_loss)+1), train_loss, label='Training Loss')
    plt.plot(range(1, len(val_loss)+1), val_loss, label='Validation Loss')
    plt.xlabel('epochs')
    plt.ylabel('loss')
    plt.xlim(0, len(train_loss)+1)
    plt.grid(True)
    plt.legend()
    plt.tight_layout()
    fig.savefig(globals.MODELS_DIR + '/loss_plot_' + pipeline_id +
                '.png', bbox_inches='tight')
