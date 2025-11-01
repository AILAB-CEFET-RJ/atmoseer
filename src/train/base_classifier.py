import yaml
from sklearn.metrics import classification_report

from train.base_forecaster import BaseForecaster
from train.evaluate import export_confusion_matrix_to_latex


class BaseClassifier(BaseForecaster):
    def print_evaluation_report(self, pipeline_id, test_loader, forecasting_task):
        print("\\begin{verbatim}")
        print(f"***Evaluation report for pipeline {pipeline_id}***")
        print("\\end{verbatim}")

        print("\\begin{verbatim}")
        print("***Hyperparameters***")
        with open("./config/config.yaml", "r") as file:
            config = yaml.safe_load(file)
        model_config = config["training"]["oc"]
        pretty_model_config = yaml.dump(model_config, indent=4)
        print(pretty_model_config)
        print("\\end{verbatim}")

        print("\\begin{verbatim}")
        print("***Model architecture***")
        print(self.learner)
        print("\\end{verbatim}")

        print("\\begin{verbatim}")
        print("***Confusion matrix***")
        print("\\end{verbatim}")
        y_true, y_pred = self.evaluate(test_loader)
        assert y_true.shape == y_pred.shape
        export_confusion_matrix_to_latex(y_true, y_pred, forecasting_task)

        print("\\begin{verbatim}")
        print("***Classification report***")
        print(classification_report(y_true, y_pred))
        print("\\end{verbatim}")
