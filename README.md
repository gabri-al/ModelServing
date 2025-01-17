## MLflow Demo
This repository is used to showcase MLflow on the Airbnb dataset.
After some preliminary data preparation steps, the following process is tracked via MLflow:
* Model tuning (fugue tune and spark cv are used)
* Model selection
* Model registry and serving, using pyfunc

A Databricks apps has been added (using Dash framework), allowing to perform inference on the pyfunc model.
