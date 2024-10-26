
![image](https://github.com/user-attachments/assets/d0638bc7-6591-4c0c-88d3-96e985515b69)


# SQL 
```
CREATE OR REPLACE STAGE mystage
url = 's3://<BUCKET>/archived_data/'
CREDENTIALS = (
    AWS_KEY_ID = 'X'
    AWS_SECRET_KEY = 'XX'
    AWS_TOKEN = ''
) ;


ALTER WAREHOUSE COMPUTE_WH SET WAREHOUSE_SIZE = 'LARGE';

-- Table without Clustering:
CREATE OR REPLACE TABLE PUBLIC.cust_no_cluster(
    C_CUSTOMER_SK NUMBER,
    C_CUSTOMER_ID VARCHAR(16),
    C_CURRENT_CDEMO_SK NUMBER,
    C_CURRENT_HDEMO_SK NUMBER,
    C_CURRENT_ADDR_SK NUMBER,
    C_FIRST_SHIPTO_DATE_SK NUMBER,
    C_FIRST_SALES_DATE_SK NUMBER,
    C_SALUTATION VARCHAR(10),
    C_FIRST_NAME VARCHAR(20),
    C_LAST_NAME VARCHAR(30),
    C_PREFERRED_CUST_FLAG CHAR(1),
    C_BIRTH_DAY NUMBER,
    C_BIRTH_MONTH NUMBER,
    C_BIRTH_YEAR NUMBER,
    C_BIRTH_COUNTRY VARCHAR(20),
    C_LOGIN VARCHAR(13),
    C_EMAIL_ADDRESS VARCHAR(50),
    C_LAST_REVIEW_DATE NUMBER
);

INSERT INTO PUBLIC.cust_no_cluster
SELECT *
FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.customer;



CREATE OR REPLACE FILE FORMAT parquet_uncompressed
TYPE = PARQUET
COMPRESSION = NONE;


COPY INTO @mystage/cust_no_cluster/
    FROM PUBLIC.cust_no_cluster
    FILE_FORMAT = (FORMAT_NAME = 'parquet_uncompressed')
    HEADER = TRUE
    OVERWRITE = TRUE;


ALTER WAREHOUSE COMPUTE_WH SET WAREHOUSE_SIZE = 'X-SMALL';


```


# Hudi Job
```
try:
    import json
    import uuid
    import os
    import boto3

except Exception as e:
    pass

global AWS_ACCESS_KEY
global AWS_SECRET_KEY
global AWS_REGION_NAME

client = boto3.client("emr-serverless"
                      )


def lambda_handler_test_emr(event, context):
    # ------------------Hudi settings ---------------------------------------------
    glue_db = "default"
    table_name = "customers"
    op = "UPSERT"
    table_type = "COPY_ON_WRITE"

    record_key = 'C_CUSTOMER_ID'
    precombine = "C_CUSTOMER_SK"
    target_path = "s3://<BUCKET>silver/cust_no_cluster/"
    raw_path = "s3://<BUCKET>/archived_data/cust_no_cluster/"
    partition_feild = "C_BIRTH_COUNTRY"
    MODE = 'METADATA_ONLY'  # FULL_RECORD  | METADATA_ONLY

    # ---------------------------------------------------------------------------------
    #                                       EMR
    # --------------------------------------------------------------------------------
    ApplicationId = "00fnep9pgvg4it09"
    ExecutionTime = 600
    ExecutionArn = "arn:aws:iam::XXX:role/EMRServerlessS3RuntimeRole"
    JobName = 'delta_streamer_bootstrap_{}'.format(table_name)

    # --------------------------------------------------------------------------------

    spark_submit_parameters = ' --conf spark.jars=/usr/lib/hudi/hudi-utilities-bundle.jar'
    spark_submit_parameters += ' --conf spark.serializer=org.apache.spark.serializer.KryoSerializer'
    spark_submit_parameters += ' --class org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer'

    arguments = [
        '--run-bootstrap',
        "--target-base-path", target_path,
        "--target-table", table_name,
        "--table-type", table_type,
        "--hoodie-conf", "hoodie.bootstrap.base.path={}".format(raw_path),
        "--hoodie-conf", "hoodie.datasource.write.recordkey.field={}".format(record_key),
        "--hoodie-conf", "hoodie.datasource.write.precombine.field={}".format(precombine),
        "--hoodie-conf", "hoodie.datasource.write.partitionpath.field={}".format(partition_feild),
        "--hoodie-conf", "hoodie.datasource.write.keygenerator.class=org.apache.hudi.keygen.SimpleKeyGenerator",
        "--hoodie-conf",
        "hoodie.bootstrap.full.input.provider=org.apache.hudi.bootstrap.SparkParquetBootstrapDataProvider",
        "--hoodie-conf",
        "hoodie.bootstrap.mode.selector=org.apache.hudi.client.bootstrap.selector.BootstrapRegexModeSelector",
        "--hoodie-conf", "hoodie.bootstrap.mode.selector.regex.mode={}".format(MODE),
    ]

    response = client.start_job_run(
        applicationId=ApplicationId,
        clientToken=uuid.uuid4().__str__(),
        executionRoleArn=ExecutionArn,
        jobDriver={
            'sparkSubmit': {
                'entryPoint': "local:///usr/lib/spark/examples/jars/spark-examples.jar",
                'entryPointArguments': arguments,
                'sparkSubmitParameters': spark_submit_parameters
            },
        },
        executionTimeoutMinutes=ExecutionTime,
        name=JobName,
    )
    print("response", end="\n")
    print(response)


lambda_handler_test_emr(context=None, event=None)


```

# apark query
```
path = "s3a://<BUCKET>/silver/cust_no_cluster/"

df = spark.read.format("hudi") \
    .option("hoodie.datasource.read.paths", path) \
    .option("hoodie.datasource.read.table.type", "COPY_ON_WRITE") \
    .load(path)


df.select(["C_BIRTH_COUNTRY", "C_CUSTOMER_ID"]).show()
```
