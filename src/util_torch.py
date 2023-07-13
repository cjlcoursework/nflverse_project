from sklearn.metrics import roc_curve, roc_auc_score
import matplotlib.pyplot as plt
import torch
import shap
import numpy as np


def plot_roc_curve(y_actual, y_predicted):
    # Assuming y_predicted and y_actual are the predicted and actual labels
    y_predicted = y_predicted.detach().numpy()
    y_actual = y_actual.detach().numpy()

    fpr, tpr, thresholds = roc_curve(y_actual, y_predicted)
    auc_score = roc_auc_score(y_actual, y_predicted)

    plt.plot(fpr, tpr, label='ROC Curve (AUC = {:.2f})'.format(auc_score))
    plt.plot([0, 1], [0, 1], 'k--')  # Random classifier line
    plt.xlabel('False Positive Rate')
    plt.ylabel('True Positive Rate')
    plt.title('Receiver Operating Characteristic (ROC) Curve')
    plt.legend()
    plt.show()


def shap_explainer(model, X):

    # Assuming you have a trained PyTorch model 'model' and a dataset 'X'
    model.eval()  # Set the model to evaluation mode

    # Create an explainer using the PyTorch model's forward function
    explainer = shap.DeepExplainer(model, data=X)

    # Convert PyTorch tensors to NumPy arrays
    X_np = X.numpy()

    # Compute SHAP values
    shap_values = explainer.shap_values(X_np)

    # Plotting the SHAP values
    shap.summary_plot(shap_values, X_np)
