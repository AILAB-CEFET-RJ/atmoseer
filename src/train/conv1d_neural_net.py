import torch
import torch.nn as nn
from torch.utils.data import TensorDataset
from train.base_neural_net import BaseNeuralNet
import functools
import operator

class Conv1DNeuralNet(BaseNeuralNet):
    def __init__(self, seq_length, input_size, output_size, dropout_rate=0.5):
        super(BaseNeuralNet, self).__init__()
        self.seq_length = seq_length
        self.input_size = input_size
        self.output_size = output_size
        self.dropout_rate = dropout_rate
        self.feature_extractor = self._get_feature_extractor()
        self.classifier = self._get_classifier()
    
    def _get_feature_extractor(self):
        fe = nn.Sequential(
            nn.Conv1d(in_channels=self.input_size, out_channels=16, kernel_size=3, padding=3),
            nn.ReLU(inplace=True),
            nn.Dropout1d(p=self.dropout_rate)
        )
        # https://datascience.stackexchange.com/questions/40906/determining-size-of-fc-layer-after-conv-layer-in-pytorch
        input_dim = (self.input_size, self.seq_length)
        self.num_features_before_fcnn = functools.reduce(operator.mul, list(fe(torch.rand(1, *input_dim)).shape))
        print(f"num_features_before_fcnn = {self.num_features_before_fcnn}")
        return fe

    def _get_classifier(self):
        return nn.Sequential(
            nn.Linear(in_features=self.num_features_before_fcnn, out_features=50),
            nn.ReLU(inplace=True),
            nn.Linear(50, self.output_size),
            nn.Sigmoid()
        )

    def create_dataloader(self, X, y, batch_size, weights = None):
        '''
        The X parameter is a numpy array having the following shape:
                    [batch_size, input_size, sequence_len] 

        The nn.Conv1D module expects inputs having the following shape: 
                    [batch_size, sequence_len, input_size] 
        See https://stackoverflow.com/questions/62372938/understanding-input-shape-to-pytorch-conv1d
        '''
        X = torch.from_numpy(X.astype('float64'))
        X = torch.permute(X, (0, 2, 1))
        y = torch.from_numpy(y.astype('float64'))

        if weights is None:
            ds = TensorDataset(X, y)
        else:
            ds = TensorDataset(X, y, weights)

        loader = torch.utils.data.DataLoader(ds, batch_size=batch_size, shuffle=True)

        return loader
