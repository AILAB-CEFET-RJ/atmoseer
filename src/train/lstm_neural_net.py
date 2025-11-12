import torch
import torch.nn as nn
from torch.utils.data import TensorDataset
from train.base_neural_net import BaseNeuralNet

# Needed because nn.LSTM() returns tuple of (tensor, (recurrent state))
# See https://discuss.pytorch.org/t/lstm-network-inside-a-sequential-container/19304/4
class extract_tensor(nn.Module):
    def forward(self,x):
        out, _ = x
        return out

class LstmNeuralNet(BaseNeuralNet):
    def __init__(self, seq_length, input_size, output_size, dropout_rate=0.5):
        super(BaseNeuralNet, self).__init__()
        self.seq_lenght = seq_length
        self.input_size = input_size
        self.output_size = output_size
        self.dropout_rate = dropout_rate
        self.feature_extractor = self._get_feature_extractor()
        self.classifier = self._get_classifier()

    def _get_feature_extractor(self):
        hidden_size = 32
        fe = nn.Sequential(
            nn.LSTM(input_size = self.input_size, hidden_size = hidden_size, num_layers = 2, batch_first = True),
            extract_tensor(),
            nn.ReLU(),
            nn.Dropout(p=self.dropout_rate),
        )
        self.num_features_before_fcnn = hidden_size * self.seq_lenght
        return fe

    def _get_classifier(self):
        return nn.Sequential(
            nn.Linear(in_features=self.num_features_before_fcnn, out_features=50),
            nn.ReLU(),
            nn.Linear(in_features=50, out_features=self.output_size),
            nn.Sigmoid()
         )

    def create_dataloader(self, X, y, batch_size, weights = None):
        '''
        The X parameter is a numpy array having the following shape:
                    [batch_size, sequence_len, input_size] 

        The nn.LSTM module (with 'batch_first = True') expects inputs having the following shape: 
                    [batch_size, sequence_len, input_size] 

        where:
            - sequence_length = number of timestamps
            - batch_size = size of each batch
            - input_size = the length of the vector describing each feature observed at each timestamp.

        See https://discuss.pytorch.org/t/using-lstm-after-conv1d-for-time-series-data/111140
        '''
        X = torch.from_numpy(X.astype('float64'))
        y = torch.from_numpy(y.astype('float64'))

        if weights is None:
            ds = TensorDataset(X, y)
        else:
            print(X.shape)
            print(y.shape)
            print(weights.shape)        
            ds = TensorDataset(X, y, weights)

        loader = torch.utils.data.DataLoader(ds, batch_size=batch_size, shuffle=True)

        return loader
