import torch
from torch.utils.data import Dataset, DataLoader


class CustomDataset(Dataset):
    def __init__(self, X, y):
        self.X = X
        self.y = y

    def __getitem__(self, index):
        x_sample = self.X[index]
        y_sample = self.y[index]
        return {'x': x_sample, 'y': y_sample}

    def __len__(self):
        return len(self.X)