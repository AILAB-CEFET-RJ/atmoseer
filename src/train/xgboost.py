from sklearn.ensemble import GradientBoostingClassifier
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.metrics import confusion_matrix
import pickle
from sklearn.metrics import classification_report

def _train_and_test_classifier(X_train, y_train, X_test, y_test):

    print(f"Shapes before reshaping: ", X_train.shape, X_test.shape)
    X_train = X_train.reshape(len(X_train), -1)
    X_test = X_test.reshape(len(X_test), -1)
    print(f"Shapes after reshaping: ", X_train.shape, X_test.shape)

    y_train[y_train>0] = 1
    y_test[y_test>0] = 1

    # Create a GradientBoostingClassifier object with default hyperparameters
    clf = GradientBoostingClassifier()

    # Train the classifier
    clf.fit(X_train, y_train)
    
    # Make predictions on the testing data
    y_pred = clf.predict(X_test.reshape(len(X_test), -1))
    
    y_true = y_test
    y_true[y_true>0] = 1

    # Calculate the confusion matrix
    cm = confusion_matrix(y_true, y_pred)

    return cm, y_true, y_pred

def report_results(cm, y_true, y_pred, title = None):
    # Define the class labels
    class_names = ['Negative', 'Positive']

    # Create a heatmap using Seaborn
    sns.heatmap(cm, annot=True, fmt='d', cmap='Blues', xticklabels=class_names, yticklabels=class_names)

    # Add labels and title
    plt.xlabel('Predicted label')
    plt.ylabel('True label')

    if title is not None:
        plt.title('Confusion Matrix - ' + title)
    else:
        plt.title('Confusion Matrix')

    # Show the plot
    plt.show()

    # Build the classification report
    target_names = ['Negative', 'Positive']
    report = classification_report(y_true, y_pred, target_names=target_names)

    # Print the classification report
    print(report)

def train_and_test_classifier(pipeline_id):
    filename = f"../data/datasets/{pipeline_id}.pickle"
    print(f"Loading train/val/test datasets from {filename}.")
    file = open(filename, 'rb')
    (X_train, y_train, X_val, y_val, X_test, y_test) = pickle.load(file)
    print(f"Shapes of train/val/test data matrices: {X_train.shape}/{X_val.shape}/{X_test.shape}")
    cm, y_true, y_pred = _train_and_test_classifier(X_train, y_train, X_test, y_test)
    report_results(cm, y_true, y_pred, pipeline_id)