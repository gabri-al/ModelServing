# Databricks notebook source
# MAGIC %md
# MAGIC # AirBnb NY Listing Price Prediction: ML Best Models

# COMMAND ----------

# MAGIC %md
# MAGIC Useful tutorial links:
# MAGIC *  tutorial [link](https://mlflow.org/docs/latest/traditional-ml/serving-multiple-models-with-pyfunc/notebooks/MME_Tutorial.html)
# MAGIC *  https://docs.databricks.com/en/machine-learning/model-serving/custom-models.html
# MAGIC *  https://docs.databricks.com/en/machine-learning/model-serving/score-custom-model-endpoints.html
# MAGIC *  Notebook with example of how to serve and query a custom model https://docs.databricks.com/en/_extras/notebooks/source/machine-learning/deploy-mlflow-pyfunc-model-serving.html 
# MAGIC

# COMMAND ----------

# MAGIC %pip install mlflow
# MAGIC %pip install scikit-learn==1.4.1.post1

# COMMAND ----------

import mlflow
from mlflow.models import infer_signature
import pandas as pd
import random
import numpy
import json

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import test data

# COMMAND ----------

catalog_ = f"price_prediction"
schema_ = f"ny_listing"
spark.sql("USE CATALOG "+catalog_)
spark.sql("USE SCHEMA "+schema_)

# COMMAND ----------

SEED_ = 155
gold_data = spark.sql("SELECT * from gold_data")
trainDF, testDF = gold_data.randomSplit([.85, .15], seed = SEED_)
ptestDF = testDF.select("*").toPandas()
display(ptestDF.head(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pyfunc: Create a custom class defining the predict method

# COMMAND ----------

class CombiningModels(mlflow.pyfunc.PythonModel):
    
    # initialize data
    def __init__(self, uri_models, weights):
        self.uri_models = uri_models # list needed
        self.weights = [float(w) for w in weights] # list needed
        self.models = []
    
    # load models from URI
    def load_models(self):
        for i in range(len(self.uri_models)):
            loaded_model_ = mlflow.pyfunc.load_model(self.uri_models[i])
            self.models.append(loaded_model_)
            #print("Model %s loaded!" % str(i))

    # create a custom predict method
    def predict(self, context, input_data): # context required as part of the API input
        '''input_data received as {'dataframe_split': df.to_dict(orient='split')} and then json.dumps() by the endpoint API but automatically converted to DataFrame'''
        final_pred = .0
        # custom predict logic
        for i in range(len(self.models)):
            model_ = self.models[i]
            pred_ = model_.predict(input_data)
            print("Model %s prediction: %.2f" % (str(i), pred_))
            final_pred += pred_ * self.weights[i]
        return final_pred

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test the custom model

# COMMAND ----------

## Pick a random datapoint from test set
rnd_ = random.randint(0, len(ptestDF))

xTest = ptestDF.iloc[[rnd_], 1:-1] # Pandas df of 1 row
yTest = ptestDF.loc[rnd_, 'price']

print("Random point picked:")
display(xTest)

#This is how the API input will look like
xTest_dict = {'dataframe_split': xTest.to_dict(orient='split')}
xTest_api = json.dumps(xTest_dict)
print(xTest_api)

# COMMAND ----------

## Prepare class parameter and input data
weights_ = [0.4, 0.6]
uri_models_ = [
  "runs:/3c07ac2132a64ea2adfe6b14471c22ad/model", # RF Model
  "runs:/892c69ea5f124e61b8efc8f36c5761e4/model" # XGB Model
]
input_data_df = xTest.copy()
input_data = xTest_api

## Initialize object from class
myCustomModel = CombiningModels(uri_models = uri_models_, weights = weights_)
myCustomModel.load_models()

# COMMAND ----------

# Perform predictions

# Raw prediction from registered models
RF_loaded_model_ = mlflow.pyfunc.load_model(uri_models_[0])
XGB_loaded_model_ = mlflow.pyfunc.load_model(uri_models_[1])
print("Original RF model prediction: %.3f" % RF_loaded_model_.predict(xTest))
print("Original XGB model prediction: %.3f" % XGB_loaded_model_.predict(xTest))

# Custom predictions from Pyfunc, inferring signature
print("\nGenerating prediction via pyfunc:")
customPred = myCustomModel.predict('', xTest)
print("Final pyfunc custom prediction : %.3f" % (customPred))
signature_ = infer_signature(xTest, customPred)

# True price value
print("\nTrue price for random point: %.3f" % yTest)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Register pyfunc model

# COMMAND ----------

# Save model into a run
experiment_ = mlflow.set_experiment("/Users/gabriele.albini@databricks.com/ModelServing_mlflow/NY_Price_Listings_Testing")
with mlflow.start_run(experiment_id=experiment_.experiment_id, run_name="Pyfunc_Model") as run:
  mlflow.pyfunc.log_model("Pyfunc_NY_CustomModel",
                          python_model = myCustomModel,
                          pip_requirements = ["pandas","numpy", "scikit-learn==1.4.1.post1"],
                          signature = signature_)
  run_id = mlflow.active_run().info.run_id

print(run_id)

# COMMAND ----------

# Copy paste mlflow code to register model
run_id = '2d78e9be1a8b4c1280256240e81748e1'
catalog = "gabrielealbini_catalog"
schema = "airbnb"
model_name = "Pyfunc_NY_CustomModel"
mlflow.set_registry_uri("databricks-uc")
mlflow.register_model(
    model_uri="runs:/"+run_id+"/Pyfunc_NY_CustomModel",
    name=f"{catalog}.{schema}.{model_name}"
)
