from train.base_learner import BaseLearner
import torch.nn as nn
from train.early_stopping import EarlyStopping
import numpy as np

class BaseNeuralNet(nn.Module, BaseLearner):
    def fit(self, n_epochs, optimizer, train_loader, val_loader, patience, criterion, pipeline_id):
        # to track the training loss as the model trains
        train_losses = []
        # to track the validation loss as the model trains
        valid_losses = []
        # to track the average training loss per epoch as the model trains
        avg_train_losses = []
        # to track the average validation loss per epoch as the model trains
        avg_valid_losses = []

        # initialize the early_stopping object
        early_stopping = EarlyStopping(patience=patience, verbose=True)

        for epoch in range(n_epochs):

            ###################
            # train the model #
            ###################
            self.train()  # prep model for training
            # for data, target in train_loader:
            for data, target, sample_weights in train_loader:
                # clear the gradients of all optimized variables
                optimizer.zero_grad()

                # forward pass: compute predicted outputs by passing inputs to the model
                output = self(data.float())

                # calculate the loss
                loss = criterion(output, target.float())
                assert not (np.isnan(loss.item()) or loss.item() >
                            1e6), f"Loss explosion: {loss.item()}"

                # see https://discuss.pytorch.org/t/per-class-and-per-sample-weighting/25530
                loss = loss * sample_weights
                loss = (loss * sample_weights / sample_weights.sum()).sum()
                loss.mean().backward()
                # loss.backward()

                # perform a single optimization step (parameter update)
                optimizer.step()

                # record training loss
                train_losses.append(loss.mean().item())

            ######################
            # validate the model #
            ######################
            self.eval()  # prep model for evaluation
            for data, target, sample_weights in val_loader:
                # forward pass: compute predicted outputs by passing inputs to the model
                output = self(data.float())
                # calculate the loss
                loss = criterion(output, target.float())
                loss = loss * sample_weights
                # record validation loss
                valid_losses.append(loss.mean().item())

            # print training/validation statistics
            # calculate average loss over an epoch
            train_loss = np.average(train_losses)
            valid_loss = np.average(valid_losses)
            avg_train_losses.append(train_loss)
            avg_valid_losses.append(valid_loss)

            epoch_len = len(str(n_epochs))

            print_msg = (f'[{(epoch+1):>{epoch_len}}/{n_epochs:>{epoch_len}}] ' +
                         f'train_loss: {train_loss:.5f} ' +
                         f'valid_loss: {valid_loss:.5f}')

            print(print_msg)

            # clear lists to track next epoch
            train_losses = []
            valid_losses = []

            early_stopping(valid_loss, self, pipeline_id)

            if early_stopping.early_stop:
                print("Early stopping activated!")
                break

        return avg_train_losses, avg_valid_losses

    def forward(self, x):
        out = self.feature_extractor(x)
        out = out.reshape(out.shape[0], -1)
        out = self.classifier(out)
        return out

    # def training_step(self, batch):
    #     images, labels = batch 
    #     out = self(images)                  # Generate predictions
    #     loss = F.cross_entropy(out, labels) # Calculate loss
    #     return loss
    
    # def validation_step(self, batch):
    #     images, labels = batch 
    #     out = self(images)                    # Generate predictions
    #     loss = F.cross_entropy(out, labels)   # Calculate loss
    #     acc = accuracy(out, labels)           # Calculate accuracy
    #     return {'val_loss': loss.detach(), 'val_acc': acc}
        
    # def validation_epoch_end(self, outputs):
    #     batch_losses = [x['val_loss'] for x in outputs]
    #     epoch_loss = torch.stack(batch_losses).mean()   # Combine losses
    #     batch_accs = [x['val_acc'] for x in outputs]
    #     epoch_acc = torch.stack(batch_accs).mean()      # Combine accuracies
    #     return {'val_loss': epoch_loss.item(), 'val_acc': epoch_acc.item()}
    
    def epoch_end(self, epoch, result):
        print("Epoch [{}], train_loss: {:.4f}, val_loss: {:.4f}, val_acc: {:.4f}".format(
            epoch, result['train_loss'], result['val_loss'], result['val_acc']))