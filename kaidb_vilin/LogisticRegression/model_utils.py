#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Model Core For Logistic Regression

import numpy as np
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import scipy
import os
import math

from sklearn.metrics import f1_score
from sklearn.metrics import roc_curve, auc,confusion_matrix

# Sigmoid Function
def g(z):
    """
    This function computes the sigmoid function across all values of z

    Argument:
    z -- numpy array of real numbers

    Returns:
    sigmoid(z)
    """
    
    return 1 / (1 + np.exp(-z))

# Hypothesis Function
def h(b, w ,X):
    """
    This function implments the logistic regression hypothesis function

    Argument:
    b -- bias
    w -- predictive parameters
    X -- data matrix of size (numbers_examples, number_predictors)

    Returns:
    sigmoid(Xw + b)
    """
    return g( (X @ w) + b)

# Weight Initialization for p predictors
def initialize(p):
    """
    This function initializes the predictive parameters to all 0. 

    Argument:
    p -- Number of predictors 

    Returns:
    w -- Zero vector of shape (p, 1) (column vector)
    b -- bias (initialized to 0) (scalar)
    """
    
    w = np.zeros((p,1))
    b = 0.

    # Ensure the model corectly initializes all parameters 
    assert(w.shape == (p, 1))
    assert(isinstance(b, float) or isinstance(b, int))
    return  b, w


# Velocity initialization for Momentum
def initialize_velocity(b,w):
    """
    Initializes the velocity for momentum
    
    Arguments:
    b -- bias 
    w -- predictive parameters 
    
    Returns:
    v -- python dictionary containing velocity
    """
    v  = {}
    v["dw"] = np.zeros( w.shape)
    v["db"] = np.zeros( (1,1))
    return v

# Cross Entropy Loss function for Binary Classification
def computeCost(b, w, X, Y, lmbda = 0): 
    """
    Computes Cross Entropy Loss function 

    Arguments:
    b -- bias
    w -- predictive parameters
    X -- data matrix of size (numbers_examples, number_predictors)
    Y -- Ground truth labels of size (number_examples, 1)
    lmbda -- regularization hyperparameter
    
    Return:
    cost -- negative log-likelihood cost for logistic regression
    """

    m = Y.size
    term1 = np.dot(-np.array(Y).T,np.log(h(b,w,X)))
    term2 = np.dot((1-np.array(Y)).T,np.log(1-h(b,w,X)))
    reg = (1/2)* lmbda * w.T @ w
    return np.squeeze( (1./m) * ( np.sum(term1 - term2) )  + reg)

# Mini Batch Partition
def random_mini_batches(X, Y, mini_batch_size = 64, seed = 0):
    """
    Creates a list of random minibatches from (X, Y)
    SOURCE: Stack overflow https://stackoverflow.com/questions/40710169/how-to-use-mini-batch-instead-of-sgd
    
    Arguments:
    X -- Datamatrix of shape (m,p) (m examples, p predictors)
    Y -- Ground Truth vector of shape (m,1)
    mini_batch_size -- size of the mini-batches, integer
    
    Returns:
    mini_batches -- list of tuples (mini_batch_X_i, mini_batch_Y_i) for i =0...len(mini_batches)-1
    """
    
    np.random.seed(seed)            # To make your "random" minibatches the same as ours
    m = X.shape[0]                  # number of training examples
    mini_batches = []
        
    # Permute the dataset synchronously 
    permutation = list(np.random.permutation(m))
    shuffled_X = X[permutation,:]
    shuffled_Y = Y[ permutation,:].reshape((m,1))

    # Divide (X,Y) into chunks of mini_batch_size
    num_complete_minibatches = math.floor(m/mini_batch_size) # number of mini batches of size mini_batch_size in your partitionning
    for k in range(0, num_complete_minibatches):
        mini_batch_X = shuffled_X[ k * mini_batch_size : (k+1) * mini_batch_size,:]
        mini_batch_Y = shuffled_Y[ k * mini_batch_size : (k+1) * mini_batch_size,:]
        mini_batch = (mini_batch_X, mini_batch_Y)
        mini_batches.append(mini_batch)
    
    # Edge Case (IE, your dataset isn't a perfect multiple of batch_size 
    if m % mini_batch_size != 0:
        mini_batch_X = shuffled_X[ num_complete_minibatches*mini_batch_size:: ,:]
        mini_batch_Y = shuffled_Y[ num_complete_minibatches*mini_batch_size:: ,:]
        mini_batch = (mini_batch_X, mini_batch_Y)
        mini_batches.append(mini_batch)
    
    return mini_batches



def max_f1(prob_preds, labels):
    """
    Finds the threshold of classification that maximizes the F1 Metric
    
    Arguments: 
    prob_preds -- numpy array of probability scores 
    labels -- ground truth labels
    
    Return:
    cutoff -- Cutoff that maximizes the F1 metric
    max_f1 -- maximal value for the f1 metric
    """
    cutoff = 0
    max_f1 = 0
    for i in prob_preds:
        preds = prob_preds > i
        tmp_score = f1_score(labels, preds)
        if tmp_score > max_f1:
            max_f1 = tmp_score
            cutoff = i
    return cutoff, max_f1

def find_closest_index(cutoff, thresholds):
    """
    Finds the threshold closest to the cutoff. 
    
    Arguments:
    cutoff -- scalar cutoff value
    threshholds -- list of all unique probabilities 
    TODO: implement this using Binary Search. YA boy is lazy. 
    
    Return:
    best_index -- index of thresholds that has a value closest to cutoff
    TODO: Implement with binary search. 
    """
    best_index = -1
    min_dist = 9999999
    for i in range(len(thresholds)):
        tmp = abs(cutoff - thresholds[i])
        if tmp < min_dist:
            min_dist = tmp
            best_index = i
    return best_index




def plot_roc(cutoff, clf,X,Y):
    prob_preds = clf.predict_prob(X)
    fpr, tpr, thresholds = roc_curve(Y, prob_preds)
    cutoff_index = find_closest_index(cutoff, thresholds)
    print("Closest threshold is", thresholds[cutoff_index])
    print("For threshold of: " ,thresholds[cutoff_index], 'FPR:', fpr[cutoff_index], "TPR:", tpr[cutoff_index])
    fig = plt.figure(figsize=(8,8))
    plt.plot(fpr,tpr)
    plt.title("ROC curve with AUC={}".format(AUC))
    plt.plot([0,1],'r--') 
    plt.scatter(fpr[cutoff_index],tpr[cutoff_index], marker='x',s=200, c='r')
    preds = np.array(prob_preds) >= cutoff
    preds = preds.astype(int)
    cm = confusion_matrix(Y, preds)
    f1 = f1_score(Y, preds)
    cm_df = pd.DataFrame(cm, index=cm_index, columns=cm_cols)
    print('F1-score',f1)
    print()
    print(cm_df)
    plt.xlabel("False Positive Rate")
    plt.ylabel("True Positive rate")
    plt.savefig("ROC.png")