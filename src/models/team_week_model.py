from tensorflow.keras import Sequential
from tensorflow.keras import regularizers
from tensorflow.keras.callbacks import EarlyStopping
from tensorflow.keras.optimizers.legacy import Adam
from tensorflow.keras.layers import Dense


def create_team_week_model(input_shape,
                           regularization_rate=0.001,
                           activation_function="relu",
                           output_function="sigmoid"):
    # Set parameters
    regularization_function = regularizers.l1(regularization_rate)

    # Create a neural network model
    model = Sequential()
    model.add(
        Dense(246, input_dim=input_shape, activation=activation_function, kernel_regularizer=regularization_function))
    model.add(Dense(164, activation=activation_function, kernel_regularizer=regularization_function))
    model.add(Dense(100, activation=activation_function, kernel_regularizer=regularization_function))
    model.add(Dense(100, activation=activation_function, kernel_regularizer=regularization_function))
    model.add(Dense(64, activation=activation_function))
    model.add(Dense(32, activation=activation_function))
    model.add(Dense(1, activation=output_function))

    return model


def train_team_week_model(model, X, y,
                          epochs_size=200,
                          batch_size=32,
                          verbose=0,
                          learning_rate=.001,
                          validation_data=None,
                          validation_split=0.15,
                          loss_function='binary_crossentropy'):
    optimizer = Adam(learning_rate=learning_rate)
    model.compile(
        optimizer=optimizer,
        loss=loss_function,
        metrics=['accuracy']
    )

    # Define the EarlyStopping callback
    early_stopping_callback = EarlyStopping(monitor='val_accuracy', patience=7, restore_best_weights=True)

    callbacks = [early_stopping_callback]

    # Train the model
    result = model.fit(X, y,
                       epochs=epochs_size,
                       batch_size=batch_size,
                       verbose=verbose,
                       callbacks=callbacks,
                       validation_data=validation_data,
                       validation_split=validation_split)

    return result


def execute_team_week_model(X, y,
                            epochs_size=200,
                            batch_size=32,
                            verbose=0,
                            learning_rate=.001,
                            activation_function="relu",
                            output_function="sigmoid",
                            loss_function='binary_crossentropy'):

    model = create_team_week_model(X.shape[1], activation_function, output_function)
    result = train_team_week_model(X, y,
                                   epochs_size=epochs_size,
                                   batch_size=batch_size,
                                   verbose=verbose,
                                   learning_rate=learning_rate,
                                   loss_function=loss_function)
    return model, result
