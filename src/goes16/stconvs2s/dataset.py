from pprint import pprint

import xarray as xr
import numpy as np

import torch

import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
from torch.utils.data import Dataset, DataLoader, WeightedRandomSampler
from tqdm import tqdm

extreme_threshold = np.log1p(50)


class NetCDFDataset(Dataset):

    def __init__(self, dataset, test_split=0, validation_split=0, is_validation=False, is_test=False, is_2d_model=False):
        super(NetCDFDataset, self).__init__()
        
        self.is_2d_model = is_2d_model
        # orignal data format batch x time x latitude x longitude x channel
        sr = Splitter(test_split, validation_split)
        if (is_test):
            data = sr.split_test(dataset)
        elif (is_validation):
            data = sr.split_validation(dataset)
        else:
            data = sr.split_train(dataset)
        
        # data format batch x channel x time x latitude x longitude
        self.X = torch.from_numpy(data.x.values).float().permute(0, 4, 1, 2, 3)
        self.X = self.X[:,:,:5,:,:]

        # take only channel 0 from data.y.values:
        only_channel_0 = data.y.values[:,:,:,:,:1]

        # self.y = torch.from_numpy(data.y.values).float().permute(0, 4, 1, 2, 3)
        self.y = torch.from_numpy(only_channel_0).float().permute(0, 4, 1, 2, 3)

        if self.is_2d_model:
            self.X = torch.squeeze(self.X)
            self.y = torch.squeeze(self.y)
                  
        del data
        
    def __getitem__(self, index):
        return (self.X[index,:,:,:,:], self.y[index])

    def __len__(self):
        return self.X.shape[0]
  
   
    
class Splitter():
    def __init__(self, test_rate, validation_rate):
        self.test_rate = test_rate
        self.validation_rate = validation_rate / (1. - self.test_rate)
        
    def split_test(self, dataset):
        return self.__split(dataset, self.test_rate, first_part=False)
        
    def split_validation(self, dataset):
        data = self.__split(dataset, self.test_rate, first_part=True)
        return self.__split(data, self.validation_rate, first_part=False)
        
    def split_train(self, dataset):
        data = self.__split(dataset, self.test_rate, first_part=True)
        return self.__split(data, self.validation_rate, first_part=True)
    
    def __split(self, dataset, split_rate, first_part):
        if split_rate:
            num_samples = dataset.sample.size
            indices = np.arange(num_samples)
            np.random.seed(42)  # or use a configurable/random seed
            np.random.shuffle(indices)
            split = int(num_samples * (1. - split_rate))
            if first_part:
                selected = indices[:split]
            else:
                selected = indices[split:]
            return dataset.isel(sample=selected)
            

def compute_sample_weights(labels, threshold=extreme_threshold):
    labels_np = labels.numpy()
    binary_labels = (labels_np >= threshold).any(axis=(1,2,3,4)).astype(int)
    print("Extreme vs non-extreme counts:", np.bincount(binary_labels))
    # [52959   729] = 52959 + 729 = 53688 = total train samples
    # assert sum(binary_labels) to be equals to total train samples
    class_counts = np.bincount(binary_labels)
    class_weights = 1.0 / class_counts
    sample_weights = class_weights[binary_labels]
    return sample_weights


def _get_bar_color(value):
    if 0 <= value < 5:
        return "lightblue"
    elif 5 <= value < 25:
        return "lightgreen"
    elif 25 <= value < 50:
        return "gold"
    elif value >= 50:
        return "tomato"
    return "lightgray"


def _get_fig_ax():
    fig, ax = plt.subplots(figsize=(6, 4))
    ax.set_xlabel("Precipitação (mm)", fontsize=10)
    ax.set_ylabel("Frequência", fontsize=10)
    ax.legend(
        handles=[
            mpatches.Patch(color="lightblue", label="Light (0, 5)"),
            mpatches.Patch(color="lightgreen", label="Moderate [5, 25)"),
            mpatches.Patch(color="gold", label="Strong [25, 50)"),
            mpatches.Patch(color="tomato", label="Extreme [50, ∞)"),
        ],
        title="Precipitation Levels",
        fontsize=8,
        title_fontsize=9,
    )
    return fig, ax


def plot_histogram(data_flatten, log=True):
    fig, ax = _get_fig_ax()
    binwidth = 5
    bins = np.arange(0, data_flatten.max() + binwidth, binwidth)
    n, bins, patches = ax.hist(data_flatten, bins=bins, edgecolor="black", log=log)
    for patch, value in zip(patches, bins):
        patch.set_facecolor(_get_bar_color(value))
    return fig, ax


if __name__ == "__main__":
    # from rafaela-model workdir:
    # python -m rafaela_model.tool.dataset
    LATS_INDEXES = slice(4, 5)
    LONS_INDEXES = slice(5, 8)

    DATASET_PATH = "/home/rionowcast/stconvs2s/data/output_10_07.nc"
    ds = xr.open_dataset(DATASET_PATH)

    validation_split = 0.2
    test_split = 0.2

    # added
    print(ds.x.channel.values)
    precipitation_x = ds.x.sel(channel='profundidade_nuvens')
    ds["x"].loc[{"channel": 'profundidade_nuvens'}] = np.log1p(precipitation_x)

    print(ds.y.channel.values)
    precipitation_y = ds.y.sel(channel='profundidade_nuvens')
    ds["y"].loc[{"channel": 'profundidade_nuvens'}] = np.log1p(precipitation_y)

    train_dataset = NetCDFDataset(ds, test_split=test_split, validation_split=validation_split)
    val_dataset   = NetCDFDataset(ds, test_split=test_split, validation_split=validation_split, is_validation=True)
    test_dataset  = NetCDFDataset(ds, test_split=test_split, validation_split=validation_split, is_test=True)

    print('[X_train] Shape:', train_dataset.X.shape)
    print('[y_train] Shape:', train_dataset.y.shape)
    print('[X_val] Shape:', val_dataset.X.shape)
    print('[y_val] Shape:', val_dataset.y.shape)
    print('[X_test] Shape:', test_dataset.X.shape)
    print('[y_test] Shape:', test_dataset.y.shape)
    print(f'''
        Train on {len(train_dataset)} samples
        Validate on {len(val_dataset)} samples
        Test on {len(test_dataset)} samples
    ''')

    test_dataset_values_greater_than_50 = (test_dataset.y >= extreme_threshold).sum()
    print("test_dataset_values_greater_than_50:", test_dataset_values_greater_than_50)

    log_data = test_dataset.y[:, 0, :, LATS_INDEXES, LONS_INDEXES].numpy().flatten()
    data_reverted = np.expm1(log_data)
    fig, ax = plot_histogram(data_reverted, log=True)

    plt.show()
    
    train_sample_weights = compute_sample_weights(train_dataset.y)
    train_sampler = WeightedRandomSampler(train_sample_weights, num_samples=len(train_sample_weights), replacement=True)

    def __init_seed(self, number=42):
        seed = (number * 10) + 1000
        np.random.seed(seed)

    params = {'batch_size': 15, 
                  'num_workers': 4, 
                  'worker_init_fn': __init_seed}

    train_loader = DataLoader(dataset=train_dataset, sampler=train_sampler, **params)
    val_loader = DataLoader(dataset=val_dataset, shuffle=False,**params)
    test_loader = DataLoader(dataset=test_dataset, shuffle=False, **params)

    train_loader_without_sampler = DataLoader(dataset=train_dataset, shuffle=True, **params)
    
    total_extreme_values_without_sampler = 0
    for X_batch, y_batch in tqdm(train_loader_without_sampler):
        total_extreme_values_without_sampler += (y_batch >= extreme_threshold).any(dim=(1,2,3,4)).sum()

    total_extreme_values_with_sampler = 0
    for X_batch, y_batch in tqdm(train_loader):
        total_extreme_values_with_sampler += (y_batch >= extreme_threshold).any(dim=(1,2,3,4)).sum()
    
    total_extreme_values_without_sampler_test = 0
    total_values_without_sampler_test = 0
    for X_batch, y_batch in tqdm(test_loader):
        # y_channel_0 = y_batch[:, 0, :, LATS_INDEXES, LONS_INDEXES] # not filtering cells
        y_channel_0 = y_batch[:, 0, :, :, :]
        # lead_time = 0
        # y_channel_0 = y_channel_0[:, slice(lead_time, lead_time + 1), :, :] # not filtering lead time
        y_channel_0 = y_channel_0[:, :, :, :]
        total_extreme_values_without_sampler_test += (y_channel_0 >= extreme_threshold).any(dim=(1,2,3)).sum()
        total_values_without_sampler_test += (y_channel_0 >= 0).any(dim=(1,2,3)).sum()
        

    pprint({
        "total_extreme_values_with_sampler": total_extreme_values_with_sampler,
        "total_extreme_values_without_sampler": total_extreme_values_without_sampler,
        "total_extreme_values_without_sampler_test": total_extreme_values_without_sampler_test,
        "total_values_without_sampler_test": total_values_without_sampler_test
    })

    pprint({
        "total_extreme_values_with_sampler": total_extreme_values_with_sampler / len(train_dataset) * 100,
        "total_extreme_values_without_sampler": total_extreme_values_without_sampler / len(train_dataset) * 100,
        "total_extreme_values_without_sampler_test": total_extreme_values_without_sampler_test / len(test_dataset) * 100,
        "total_values_without_sampler_test": total_values_without_sampler_test / len(test_dataset) * 100
    })