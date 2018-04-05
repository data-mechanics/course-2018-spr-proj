import numpy as np
from keras.models import Sequential
from keras.layers import Dense
from keras.models import model_from_json
import numpy
import os
import tensorflow as tf
import multiprocessing
import keras.backend as K
from keras.callbacks import EarlyStopping
from keras.models import Sequential
from keras.layers.core import Dense, Dropout, Flatten
from keras.layers.convolutional import Conv1D
from keras.optimizers import Adam


def build_model(max_tweet_length, vector_size):
    
    batch_size = 512
    nb_epochs = 5
    model = Sequential()
# This fucking block will eat all of your goddamn memory 
# Sacrifice 3 chickens to improve accuracy 

    model.add(Conv1D(32, kernel_size=3, activation='relu', padding='same', input_shape=(max_tweet_length, vector_size)))
    model.add(Conv1D(32, kernel_size=3, activation='relu', padding='same'))
    model.add(Conv1D(32, kernel_size=3, activation='relu', padding='same'))
    model.add(Conv1D(32, kernel_size=3, activation='relu', padding='same'))
    model.add(Dropout(0.25))
    model.add(Conv1D(32, kernel_size=2, activation='relu', padding='same'))
    model.add(Conv1D(32, kernel_size=2, activation='relu', padding='same'))
    model.add(Conv1D(32, kernel_size=2, activation='relu', padding='same'))
    model.add(Conv1D(32, kernel_size=2, activation='relu', padding='same'))
    model.add(Dropout(0.25))
    model.add(Flatten())
    model.add(Dense(256, activation='tanh'))
    model.add(Dense(2, activation='softmax'))
    # Compile the model
    model.compile(loss='categorical_crossentropy',
              optimizer=Adam(lr=0.001, decay=1e-6),
              metrics=['accuracy'])
    return model

def main():
    max_tweet_length = 30
    vector_size = 512
    # generate model 
    use_gpu = True
    config = tf.ConfigProto(intra_op_parallelism_threads=multiprocessing.cpu_count(), 
                            inter_op_parallelism_threads=multiprocessing.cpu_count(), 
                            allow_soft_placement=True, 
                            device_count = {'CPU' : multiprocessing.cpu_count(), 
                                            'GPU' : 1 if use_gpu else 0})

    session = tf.Session(config=config)
    K.set_session(session)
    model = build_model(max_tweet_length, vector_size)
    test_X = np.load('./test.npz')
    test_Y = np.load('./test.npz')
    model.load_weights("deep_nn_weights.h5",by_name=True)
    print("Generating Test Predictions ")
    probs = model.predict_proba(Test_X)
    print("Saving Probability Predictions")
    np.savez_compressed("/proba_preds", probs)

main()

