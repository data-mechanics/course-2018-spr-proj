
#!/usr/bin/env python
# -*- coding: utf-8 -*-

import dill
import keras.backend as K
import multiprocessing
import tensorflow as tf
import numpy as np
from gensim.models.word2vec import Word2Vec
import re
import time
from keras import callbacks
import matplotlib.pyplot as plt
from keras.callbacks import EarlyStopping
from keras.models import Sequential
from keras.layers.core import Dense, Dropout, Flatten
from keras.layers.convolutional import Conv1D
from keras.optimizers import Adam
import time
import multiprocessing
from keras.models import load_model
from sklearn.model_selection import train_test_split
from sklearn.feature_extraction.text import TfidfVectorizer
from tqdm import tqdm
tqdm.pandas(desc="progress-bar")


def buildWordVector(tokens, size, X_vecs, tfidf):
    vec = np.zeros(size * 3).reshape((1,3,  size))
    count = 0.
    mi = np.inf
    ma = 0
    mav = None
    miv= None
    for token in tokens:
        try:
            v = X_vecs[token]
            norm = np.linalg.norm(v)
            if norm > ma:
                ma = norm
                mav = v
            if norm < mi and norm !=0:
                m = norm
                miv = v
            vec[:,0,:] += v.reshape((1, size)) * tfidf[token]
            count += 1.
        # Token is not in corpus 
        except KeyError: 
            continue
    vec[0,1,:] = miv
    vec[0,2,:] = mav
    vec = vec.reshape( (1, 3* size))
    #if count != 0:
    # will delete nans later 
    vec /= count
    #else:
        #return []
     #   pass
        #print("Null vector, should trim ")
    return vec

# def build_model(vector_size):
    
#     batch_size = 512
#     nb_epochs = 5
#     model = Sequential()
# # This fucking block will eat all of your goddamn memory 
# # Sacrifice 3 chickens to improve accuracy 

#     model.add(Conv1D(32, kernel_size=3, activation='relu', padding='same', input_shape=(vector_size*3,1)))
#     model.add(Conv1D(32, kernel_size=3, activation='relu', padding='same'))
#     model.add(Conv1D(32, kernel_size=3, activation='relu', padding='same'))
#     model.add(Conv1D(32, kernel_size=3, activation='relu', padding='same'))
#     model.add(Dropout(0.25))
#     model.add(Conv1D(32, kernel_size=2, activation='relu', padding='same'))
#     model.add(Conv1D(32, kernel_size=2, activation='relu', padding='same'))
#     model.add(Conv1D(32, kernel_size=2, activation='relu', padding='same'))
#     model.add(Conv1D(32, kernel_size=2, activation='relu', padding='same'))
#     model.add(Dropout(0.25))
#     model.add(Flatten())
#     model.add(Dense(64, activation='tanh'))
#     model.add(Dense(64, activation='tanh'))
#     model.add(Dense(1, activation='sigmoid'))
#     # Compile the model
#     model.compile(loss='binary_crossentropy',
#               optimizer=Adam(lr=0.001, decay=1e-6),
#               metrics=['accuracy'])
#     return model

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
    batch_size = 64
    
    model = load_model("mean_tfidf_max_min_dnn.h5")
    tbCallBack= callbacks.TensorBoard(log_dir='./LOGS', histogram_freq=0, batch_size=batch_size, write_graph=True, write_grads=False, write_images=False, embeddings_freq=0, embeddings_layer_names=None, embeddings_metadata=None)
    print("Loaded DNN model to update")
    print("Model Loaded ")
    # continue building
    keras_model = "deep_nn_weights.h5"
    model_name = 'tweet_word2vec.model'
    # static names 
    dataset_location = './Sentiment Analysis Dataset.csv'
    model_location = './model/'
    tokenized_corpus_name = "tokenized_tweet_corpus.dill"
    groun_truth_name  = 'ground_truth_tokenized_tweet_corpus.dill'
    model_name = 'tweet_word2vec.model'


    # Load all data
    with open(model_location + tokenized_corpus_name, 'rb') as f:
        tokenized_corpus = dill.load(f)

    with open(model_location + groun_truth_name, 'rb') as f:
        ground_truth = dill.load(f)

    # Load model and retrieve word vectors 
    word2vec = Word2Vec.load(model_location + model_name)
    X_vecs = word2vec.wv
    nb_epochs = 5
    test_size = 100000
    validation_size = 100000
    train_size =  len(tokenized_corpus) -  test_size - validation_size
    print("Train Size:{}, Validation Size:{}, Test Size:{}".format(train_size, validation_size, test_size))
    X_corp_train, X_corp_valid, Y_train, Y_valid = train_test_split( tokenized_corpus, ground_truth, test_size=0.10, random_state=69)
    vectorizer = TfidfVectorizer(analyzer=lambda x: x, min_df=10)
    matrix = vectorizer.fit_transform([x for x in X_corp_train])
    tfidf = dict(zip(vectorizer.get_feature_names(), vectorizer.idf_))
    print( 'vocab size :', len(tfidf))
    train_vecs_w2v = np.concatenate([buildWordVector(z, 512, X_vecs, tfidf) for z in tqdm(X_corp_train)])
    valid_vecs_w2v = np.concatenate([buildWordVector(z, 512, X_vecs, tfidf) for z in tqdm(X_corp_valid)])
    dummy_valid = [~np.isnan(train_vecs_w2v).any(axis=1)]
    train_vecs_w2v = train_vecs_w2v[dummy_valid]
    # convert back to tensor 
    train_vecs_w2v = train_vecs_w2v.reshape( (train_vecs_w2v.shape[0], train_vecs_w2v.shape[1], 1) )
    Y_train = np.array(Y_train)
    Y_train = Y_train.reshape((len(Y_train),1))
    Y_train = Y_train[dummy_valid]

    dummy_valid = [~np.isnan(valid_vecs_w2v).any(axis=1)]
    valid_vecs_w2v = valid_vecs_w2v[dummy_valid]
    # convert to tensor 
    valid_vecs_w2v = valid_vecs_w2v.reshape((valid_vecs_w2v.shape[0], valid_vecs_w2v.shape[1], 1))
    Y_valid = np.array(Y_valid)
    Y_valid = Y_valid.reshape((len(Y_valid),1))
    Y_valid = Y_valid[dummy_valid]

    del dummy_valid
    #dump_files = [X_train, Y_train, X_valid, Y_valid, X_test, Y_test ]
    # Super fucing memoery intensive 
    print("Dataset has been created ")
    print("DATA SHAPE: {}".format(train_vecs_w2v.shape))

    model.fit(train_vecs_w2v, Y_train,
          batch_size=batch_size,
          shuffle=True,
          epochs=nb_epochs,
          validation_data=(valid_vecs_w2v, Y_valid),
          callbacks=[EarlyStopping(min_delta=0.00025, patience=2), tbCallBack])
    print("Model complete, saving")
    #model.save_weights('contin_deep_nn_2_weights.h5')
    model.save("adj_mean_tfidf_max_min_dnn.h5")


if __name__ == "__main__":
    main()
