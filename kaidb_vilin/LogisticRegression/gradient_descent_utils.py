#!/usr/bin/env python
# -*- coding: utf-8 -*-
from model_utils import *

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import scipy
import os
import math

from sklearn.metrics import f1_score
from sklearn.metrics import roc_curve, auc,confusion_matrix

# Gradient Descent Core 


def computeGradients(b, w, X, Y, lmbda):
    """
    Computes gradients

    Arguments:
    w -- weight parameters
    b -- bias
    X -- datamatrix
    Y -- ground truth labels 
    lmbda -- regularization hyperparameter

    Return:
    cost -- Loss function cose
    dw -- gradient of the loss with respect to predictors w
    db -- gradient of the loss with respect to bias b    
    """
    # number of examples
    m = X.shape[0]
    # Hypothesis predictions 
    hx = h(b,w,X)
    cost = computeCost(b, w, X,Y)
    dw =  (1/m ) *  X.T @ (hx -Y) + (lmbda * w) 
    db = (1/m ) *  np.sum(hx - Y)

    assert(dw.shape == w.shape)
    assert(db.dtype == float)
    # Cost should be scalar
    cost = np.squeeze(cost)
    assert(cost.shape == ())
    
    grads = {"dw": dw,
             "db": db}
    # store cost to track its value as a function of iterations 
    return grads, cost




def compute_update_with_momentum(b,w, grads, v, beta, learning_rate):
    """
    Update parameters using Momentum
    
    Arguments:
    parameters -- python dictionary containing your parameters:
                    parameters['W' + str(l)] = Wl
                    parameters['b' + str(l)] = bl
    grads -- python dictionary containing your gradients for each parameters:
                    grads['dW' + str(l)] = dWl
                    grads['db' + str(l)] = dbl
    v -- python dictionary containing the current velocity:
                    v['dW' + str(l)] = ...
                    v['db' + str(l)] = ...
    beta -- the momentum hyperparameter, scalar
    learning_rate -- the learning rate, scalar
    
    Returns:
    parameters -- python dictionary containing your updated parameters 
    v -- python dictionary containing your updated velocities
    """

    # Momentum update for each parameter
    dw, db = grads["dw"], grads["db" ]
    vdw, vdb = v["dw"], v["db"]
    # compute velocities
    v["dw"] = (beta * vdw) + (1 - beta)*dw
    v["db"] = (beta * vdb) + (1 - beta)*db
    # update parameters
    w -= learning_rate * v["dw"]
    b -= learning_rate * v["db"]

    return b,w, v



def gradientDescent(b, w,  X, Y,lmbda, num_iterations, learning_rate, verbose = False, stopping_tolerance = 1e-8):
    """
    This function calculates w,b  by running a gradient descent for num_iterations. 
    
    Arguments:
    b -- bias, 
    w -- predictors
    X -- datamatrix
    Y -- ground truth labels
    lmbda -- regularization hyperparameter
    num_iterations -- number of times to run GD 
    learning_rate -- values for alpha. 
    verbose --Log the loss every 100 steps
    
    Returns:
    b_optimal -- optimized bias 
    w_optimal -- optimized predictor
    costs -- Array of 
    
    TODO: add early stoping, optimization options, regularization 
    """
    
    costs = []
    tol=0
    for i in range(num_iterations):
        # Compute Gradient and cost
        grads, cost = computeGradients(b, w, X, Y, lmbda)
        # Retrieve derivatives from grads
        dw = grads["dw"]
        db = grads["db"]
        
        # Gradient update
        w_old = w
        b_old = b
        
        w = w - learning_rate * (dw)
        b = b - learning_rate * (db)
        if np.linalg.norm(w_old - w) < stopping_tolerance:
            tol+=1
            if tol>10:
                print("Early stopping triggered at itteration {}".format(i))
                costs.append(cost)
                break
        else:
            tol=0
        # Log cost (every 100 iterations )
        if i % 100 == 0:
            costs.append(cost)
            if verbose:
                print ("Loss Value after iteration {}: {}".format(i, cost))
    
    b_optimal, w_optimal = b, w
    return b_optimal, w_optimal, costs


def momentumGradientDescent(b, w,v,  X, Y,lmbda, num_iterations, learning_rate,beta, verbose = False, stopping_tolerance = 1e-6):
    """
    This function calculates w,b  by running a gradient descent for num_iterations. 
    
    Arguments:
    b -- bias, 
    w -- predictors
    v -- velocity parameters 
    X -- datamatrix
    Y -- ground truth labels
    lmbda -- regularization hyperparameter
    num_iterations -- number of times to run GD 
    learning_rate -- values for alpha. 
    beta -- momentum
    verbose --Log the loss every 100 steps
    
    Returns:
    b_optimal -- optimized bias 
    w_optimal -- optimized predictor
    costs -- Array of 
    
    TODO: add early stoping, optimization options, regularization 
    """
    
    costs = []
    tol=0
    for i in range(num_iterations):
        # Compute Gradient and cost
        grads, cost = computeGradients(b, w, X, Y, lmbda)
        # Retrieve derivatives from grads
        dw = grads["dw"]
        db = grads["db"]
        
        # Gradient update
        w_old = w
        b_old = b
        b,w,v = compute_update_with_momentum(b,w, grads, v, beta, learning_rate)
    
        if np.linalg.norm(w_old - w) < stopping_tolerance:
            tol+=1
            if tol>1000:
                print("Early stopping triggered at itteration {}".format(i))
                costs.append(cost)
                break
        else:
            tol=0
        # Log cost (every 100 iterations )
        if i % 1000 == 0:
            costs.append(cost)
            if verbose:
                print ("Loss Value after iteration {}: {}".format(i, cost))
    
    b_optimal, w_optimal = b, w
    return b_optimal, w_optimal, costs



def miniBatchMomentumGradientDescent(b, w,v,  X, Y,lmbda, num_iterations, learning_rate,beta, verbose = False,batch_size=64,seed=6969, stopping_tolerance = 1e-6):
    """
    This function calculates w,b  by running a gradient descent for num_iterations. 
    
    Arguments:
    b -- bias, 
    w -- predictors
    v -- velocity parameters 
    X -- datamatrix
    Y -- ground truth labels
    lmbda -- regularization hyperparameter
    num_iterations -- number of times to run GD 
    learning_rate -- values for alpha. 
    beta -- momentum
    verbose --Log the loss every 100 steps
    
    Returns:
    b_optimal -- optimized bias 
    w_optimal -- optimized predictor
    costs -- Array of 
    
    TODO: add early stoping, optimization options, regularization 
    """
    
    costs = []
    for i in range(num_iterations):
        
        # Define the random minibatches. We increment the seed to reshuffle differently the dataset after each epoch
        seed = seed + 1
        minibatches = random_mini_batches(X, Y, batch_size, seed)
        
        for minibatch in minibatches:
            (minibatch_X, minibatch_Y) = minibatch
            # Compute Gradient and cost
            grads, cost = computeGradients(b, w, minibatch_X, minibatch_Y, lmbda)
            # Retrieve derivatives from grads
            dw = grads["dw"]
            db = grads["db"]

            # Gradient update
            w_old = w
            b_old = b
            b,w,v = compute_update_with_momentum(b,w, grads, v, beta, learning_rate)

           
            # Log cost (every 100 iterations )
            if i % 1000 == 0:
                costs.append(cost)
                if verbose:
                    print ("Loss Value after iteration {}: {}".format(i, cost))

    b_optimal, w_optimal = b, w
    return b_optimal, w_optimal, costs

