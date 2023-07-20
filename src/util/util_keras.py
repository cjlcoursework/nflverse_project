
import tensorflow as tf
import matplotlib.pyplot as plt
import shap
from sklearn.metrics import roc_curve, auc


def plot_roc_curve(y_actual, y_predicted):

    # Assuming y_predicted and y_actual are the predicted and actual labels
    fpr, tpr, thresholds = roc_curve(y_actual, y_predicted)
    plt.plot(fpr, tpr)
    plt.plot([0, 1], [0, 1], 'k--')  # Random classifier line
    plt.xlabel('False Positive Rate')
    plt.ylabel('True Positive Rate')
    plt.title('Receiver Operating Characteristic (ROC) Curve')
    plt.show()


def shap_explainer(model, X):

    # Assuming you have a trained Keras model 'model' and a dataset 'X'
    explainer = shap.DeepExplainer(model, data=X)
    shap_values = explainer.shap_values(X)

    # Plotting the SHAP values
    shap.summary_plot(shap_values, X)