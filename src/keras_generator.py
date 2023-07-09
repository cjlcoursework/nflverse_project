from keras.utils import Sequence


class CustomDataGenerator(Sequence):
    def __init__(self, X, y, batch_size):
        self.X = X
        self.y = y
        self.batch_size = batch_size

    def __len__(self):
        return len(self.X) // self.batch_size

    def __getitem__(self, index):
        start = index * self.batch_size
        end = (index + 1) * self.batch_size
        batch_X = self.X[start:end]
        batch_y = self.y[start:end]
        return batch_X, batch_y
