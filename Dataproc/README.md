# Google Cloud Dataproc

## Big Query to cloud storage using PySpark in GCP Dataproc

### Setting Up Our Environment

1. First, we need to enable Cloud Dataproc, Compute Engine and the Google Cloud Dataproc APIs.

2. Next, open up Cloud Shell by clicking the button in the top right-hand corner of the cloud console:

    - We're going to set some environment variables that we can reference as we proceed with the codelab. e.g.
    
   ```
   CLUSTER_NAME=<cluster_name>
   ```
   
   - Next, choose a REGION from one of the ones available [here](https://cloud.google.com/compute/docs/regions-zones/). An example might be us-east1
   ```
   REGION=<region>
   ```

3. With our environment variables configured, let's run the following command to create our Cloud Dataproc cluster:
    ```
    gcloud beta dataproc clusters create ${CLUSTER_NAME} \
     --region=${REGION} \
     --worker-machine-type n1-standard-2 \
     --num-workers 2 \
     --image-version 1.4-debian9 \
     --initialization-actions gs://dataproc-initialization-actions/python/pip-install.sh \
     --metadata 'PIP_PACKAGES=google-cloud-storage' \
     --optional-components=ANACONDA \
     --enable-component-gateway
    ```
4. Next, run the following commands in your Cloud Shell to clone the repo with the sample code and cd into the correct directory:
    ```
    cd
    git clone https://github.com/avj10/GoogleCloud
    cd GoogleCloud/Dataproc
    ```

5. Next, we'll create a Google Cloud Storage bucket for our output files. After making a bucket, go back to your cloud shell and set an environment variable with the name of your bucket:
   ```
   BUCKET_NAME=<bucket_name>
   ```

6. Great now we are all set to submit PySpark job. Enter following command by passing `year` and `month` as arguments value. e.g. `2017` `03`
    ````
    gcloud dataproc jobs submit pyspark \
        --cluster ${CLUSTER_NAME} \
        --jars gs://spark-lib/bigquery/spark-bigquery-latest.jar \
        --driver-log-levels root=FATAL \
        bigQueryToCloudStorage.py \
        -- ${year} ${month} ${BUCKET_NAME}
    ````
   Example:
   ````
    gcloud dataproc jobs submit pyspark \
        --cluster ${CLUSTER_NAME} \
        --jars gs://spark-lib/bigquery/spark-bigquery-latest.jar \
        --driver-log-levels root=FATAL \
        bigQueryToCloudStorage.py \
        -- 2017 03 ${BUCKET_NAME}
    ````
   
7. Cleanup
    - To avoid incurring unnecessary charges to your GCP account after completion of this quickstart:
        1. [Delete the Cloud Storage bucket](https://cloud.google.com/storage/docs/deleting-buckets) for the environment and that you created.
        2. [Delete the Cloud Dataproc environment](https://cloud.google.com/dataproc/docs/guides/manage-cluster)
        3. If you created a project just for this codelab, you can also optionally delete the project.
    
##Reference
[PySpark for Preprocessing BigQuery Data](https://clmirror.storage.googleapis.com/codelabs/pyspark-bigquery/index.html?index=..%2F..index#0)