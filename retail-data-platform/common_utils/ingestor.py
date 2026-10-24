from common_utils.logging import get_logger

logger = get_logger("ingestor")


def read_jdbc(spark, url, dbtable, user, password, driver="com.microsoft.sqlserver.jdbc.SQLServerDriver"):
    """
    Read a table from a relational database over JDBC

    url       : jdbc connection url
    dbtable   : table (or "(subquery) alias") to read
    user      : username, from the secret scope
    password  : password, from the secret scope
    driver    : jdbc driver class

    returns a dataframe
    """
    logger.info("Reading %s over JDBC", dbtable)

    return (
        spark.read.format("jdbc")
        .option("url", url)
        .option("dbtable", dbtable)
        .option("user", user)
        .option("password", password)
        .option("driver", driver)
        .load()
    )


def read_cosmosdb_json(spark, connection_string, database_name, collection_name, column_name="json_data"):
    """
    Read every document of a Cosmos DB (MongoDB API) collection

    connection_string : from the secret scope
    database_name     : e.g. retail
    collection_name   : e.g. sales_orders
    column_name       : name of the single output column

    returns a dataframe with one JSON string per document
    """
    from pymongo import MongoClient
    from bson import json_util

    logger.info("Reading %s.%s from Cosmos DB", database_name, collection_name)

    client = MongoClient(connection_string)
    try:
        documents = list(client[database_name][collection_name].find({}))
    finally:
        client.close()

    logger.info("Fetched %s documents", len(documents))

    rows = [(json_util.dumps(document),) for document in documents]
    return spark.createDataFrame(rows, f"{column_name} string")


def read_s3_parquet(spark, aws_access_key_id, aws_secret_access_key, path):
    """
    Read parquet from Amazon S3

    aws_access_key_id     : from the secret scope
    aws_secret_access_key : from the secret scope
    path                  : bucket/folder/file.parquet

    returns a dataframe
    """
    logger.info("Reading s3a://%s", path)

    source_path = f"s3a://{aws_access_key_id}:{aws_secret_access_key}@{path}"
    return spark.read.parquet(source_path)


def write_raw(df, target_path, file_format, mode, options=None):
    """
    Write a dataframe as-is into the raw volume

    target_path : /Volumes/<catalog>/bronze/raw_data/<folder>/load_date=YYYY-MM-DD
    file_format : csv | json | parquet
    mode        : overwrite (idempotent per load_date) | append
    options     : writer options, e.g. header for csv

    returns the target path
    """
    logger.info("Writing %s to %s (mode=%s)", file_format, target_path, mode)

    writer = df.write.mode(mode).format(file_format)
    for key, value in (options or {}).items():
        writer = writer.option(key, value)

    writer.save(target_path)
    return target_path
