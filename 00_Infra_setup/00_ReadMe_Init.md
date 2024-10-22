# Initialization Steps to create objects from CLI

### 1-Create Catalog, Schema, Volume from Notebook
Run the `Initialize_Resources` notebook in this folder, using a Serverless cluster

### 2-Download the content of the `_files` folder

### 3-Upload files to Volume using the Databricks CLI
- Open terminal from MacOS and validate if v0.221.1 is available: `databricks --version`
- Connect to a workspace: `databricks configure --token`
- Insert the host: e.g. https://e2-demo-field-eng.cloud.databricks.com/
- Insert a PAT
- Upload csv to volume: `databricks fs cp /Users/gabriele.albini/Downloads/_files/cluster_script.sh dbfs:/Volumes/users/gabriele_albini/airbnb/`
- Upload cluster init script to volume: `databricks fs cp /Users/gabriele.albini/Downloads/_files/NY_Listing_cleaned.csv dbfs:/Volumes/users/gabriele_albini/airbnb/`

### 4-Create a Cluster using the Databricks CLI
- Launch the creation of a cluster: `databricks clusters create --json @"/Users/gabriele.albini/Downloads/_files/cluster_config.json"`

### 5-Create a Data Preparation Job
- Create a Serverless Job based on json configurations: `databricks jobs create --json @"/Users/gabriele.albini/Downloads/_files/data_prep_job_config.json"`
Alternatively, if serverless shouldn't be used:
- Take the ID from the cluster created above: `databricks clusters list`
- Locate the id for the clsuter "Gab_ML_Cluster" and add it to the job json configuration file (before the timeout seconds clause, add a new property with the cluster ID `"existing_cluster_id": "1022-145435-b40g4i27"`).
- In the notebooks, switch the way parameters are retrieved (currently they are job parameters)
