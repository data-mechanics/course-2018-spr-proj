#!/usr/bin/env python
# -*- coding: utf-8 -*-
from model_utils import *
from gradient_descent_utils import *

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import scipy
import os
import math

from sklearn.metrics import f1_score
from sklearn.metrics import roc_curve, auc,confusion_matrix

class LogisticRegression:
    def __init__(self, lmbda = 0, learning_rate = .01,beta=.9 ,num_iterations= 10000, opt='GD', seed=6969, batch_size=64):
        self.costs = None
        self.opt=opt
        self.lmbda = lmbda
        self.alpha = learning_rate
        # only used if momentum is used 
        self.beta=.9
        self.num_iterations = num_iterations
        self.seed = seed
        self.batch_size = batch_size
        
    def fit(self, X, Y, verbose=False):
        self.p = X.shape[1]
        self.b, self.w = initialize(self.p)
        opt=self.opt
        if opt =="GD":
            print("Using Gradient Descent")
            self.b,self.w , self.costs = gradientDescent(
                self.b, 
                self.w,
                X,
                Y,
                self.lmbda,
                self.num_iterations,
                self.alpha,
                verbose = verbose)
        if opt == "Momentum":
            print("Using Gradient Descent with Momentum")
            self.v = initialize_velocity(self.b,self.w)
            self.b,self.w , self.costs = momentumGradientDescent(
                self.b, 
                self.w,
                self.v,
                X,
                Y,
                self.lmbda,
                self.num_iterations,
                self.alpha,
                self.beta,
                verbose = verbose)
        if opt == "MB_Momentum":
            print("Using MiniBatch Gradient Descent with Momentum")
            self.v = initialize_velocity(self.b,self.w)
            self.b,self.w , self.costs = miniBatchMomentumGradientDescent(
                b=self.b, 
                w= self.w,
                v= self.v,
                X=X,
                Y=Y,
                lmbda = self.lmbda,
                num_iterations= self.num_iterations,
                learning_rate= self.alpha,
                beta= self.beta,
                batch_size= self.batch_size,
                seed = self.seed,
                verbose = verbose)
            

        
    def predict_prob(self, X):
        return h(self.b, self.w, X)
    
    def predict(self, X):
        preds = self.predict_prob(X)
        return np.around(preds + 1e-6) 
    
    def score(self, X, Y):
        preds = self.predict(X)
        return np.squeeze(np.sum(preds == Y) / len(Y))
    
    def plot_learning_curve(self):
        # Plot learning curve (with costs)
        plt.figure(figsize=(8,6))
        plt.plot(self.costs)
        plt.ylabel('cost')
        plt.xlabel('iterations (per hundreds)')
        plt.title("Learning rate =" + str(self.alpha))
        plt.show()


def find(name, path): #Helper method for locating files
    for root, dirs, files in os.walk(path):
        if name in files:
            return os.path.join(root, name)
            v


def test():
	# Toy Dataset for benchmarking
	path =  find('Wisconsin_breast_cancer.csv', "./" )
	data = pd.read_csv(path, index_col=0)
	features = data.columns.values[:-1]
	target = data.columns.values[-1]

	# 0 -- Benign
	# 1 -- Malignant
	target_map = {
	    'benign':0,
	    'malignant':1
	}

	X, Y = data[features], data[target]
	Y = Y.values.reshape((Y.shape[0],1))
	acc = []
	for opt_method in ["GD", "Momentum", "MB_Momentum"]:
		clf = LogisticRegression(lmbda=10, opt=opt_method)
		clf.fit(X.values,Y, verbose=True)
		acc += [clf.score(X,Y)]

	print("Accuracy for GD:{}, Momentum:{}, MiniBatch Momentum:{}". format(acc[0], acc[1], acc[2]))

# uncomment for test run and sample usage. 
#test()

        
    