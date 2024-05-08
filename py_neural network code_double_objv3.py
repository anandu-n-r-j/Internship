# -*- coding: utf-8 -*-
"""
Created on Tue Feb 13 14:42:19 2024

@author: S Gupta
"""


import pandas as pd
from sklearn.neural_network import MLPRegressor
from sklearn.model_selection import KFold, train_test_split
from sklearn.metrics import mean_squared_error, r2_score
#from SALib.analyze import sobol
#from SALib.sample import saltelli
import numpy as np

# Step 1: Load and Prepare Data
data = pd.read_csv("C:\Users\anand\Downloads\Data.csv")
X = data[['v1', 'v2', 'v3', 'v4', 'v5']]
y = data[['v6', 'v7']]

# Step 2: Neural Network Regression with k-fold Cross-Validation
kf = KFold(n_splits=10, shuffle=False)  # Define k-fold cross-validation



PRESS1=[]
PRESS2=[]


X_train_org, X_test_org, y_train_org, y_test_org = train_test_split(X, y, test_size=0.2, random_state=42)

# Initialize neural network regressor
model1 = MLPRegressor(hidden_layer_sizes=(5,5), activation='tanh', max_iter=10000,learning_rate_init=0.1)
model1.fit(X_train_org, y_train_org)

# Evaluate the model
y_pred_org = model1.predict(X_test_org)

mse_1 = mean_squared_error(y_test_org['v6'], y_pred_org[:,0])
mse_2 = mean_squared_error(y_test_org['v7'], y_pred_org[:,1])
    
r2_1 = r2_score(y_test_org['v6'], y_pred_org[:,0])
r2_2 = r2_score(y_test_org['v7'], y_pred_org[:,1])


for train_index, test_index in kf.split(X):
    X_train, X_test = X.iloc[train_index], X.iloc[test_index]
    y_train, y_test = y.iloc[train_index], y.iloc[test_index]

    # Initialize neural network regressor
    model2 = MLPRegressor(hidden_layer_sizes=(5,5), activation='tanh', max_iter=10000,learning_rate_init=0.1)

    # Train the model
    model2.fit(X_train, y_train)

    # Evaluate the model
    y_pred = model2.predict(X_test) # prediction from the k-fold model
    
    y_pred_org = model1.predict(X_test) # prediction from the reference model
    
    residual1 = np.sum((y_pred_org[:,0] - y_pred[:,0]) ** 2)
    residual2 = np.sum((y_pred_org[:,1] - y_pred[:,1]) ** 2)

        
    PRESS1.append(residual1)
    PRESS2.append(residual2)
    
    
# Check the number of samples for sensitivity analysis
# Calculate Mean Squared Error for all folds

PRESS_1 = np.sum(PRESS1)
PRESS_2 = np.sum(PRESS2)



print("Mean Squared Error for 1st obj", mse_1)
print("Mean Squared Error for 2nd obj", mse_2)

print("R2 for 1st obj", r2_1)
print("R2 for 2nd obj", r2_2)


print("PRESS for 1st obj", PRESS_1)
print("PRESS for 2nd obj", PRESS_2)
'''


problem = {
    'num_vars': 5,  # Number of parameters (independent variables)
    'names': ['v1', 'v2', 'v3', 'v4', 'v5'],  # Names of parameters
    'bounds': [[0, 1], [0,1], [0, 1], [0, 1], [0, 1]]  # Ranges of parameters
}


#param_values = saltelli.sample(problem, 1024)

column_names = ['v1', 'v2', 'v3', 'v4', 'v5']  # Adjust column names based on your actual variables

# Create a DataFrame from the array with column names
param_df = pd.DataFrame(param_values, columns=column_names)


Y=model1.predict(param_df)

Y1=np.array(Y[:,0])


Y2=np.array(Y[:,1])

S1 = sobol.analyze(problem, Y1)
S2 = sobol.analyze(problem, Y2)

print("sobol first indices for 1st obj", S1['S1'])
print("sobol total indices for 1st obj", S1['ST'])
print("sobol first indices for 1st obj", S2['S1'])
print("sobol total indices for 1st obj", S2['ST'])



'''