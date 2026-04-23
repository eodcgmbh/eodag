import os
import boto3
from botocore.exceptions import ClientError
from eodag import EODataAccessGateway
from tqdm.auto import tqdm


def set_env(S3_HOST, S3_KEY, S3_PW, CDSE_USER, CDSE_PW):
    os.environ["S3_HOST"] = S3_HOST
    os.environ["S3_KEY"] = S3_KEY
    os.environ["S3_PW"] = S3_PW
    os.environ["EODAG__COP_DATASPACE__AUTH__CREDENTIALS__USERNAME"] = CDSE_USER
    os.environ["EODAG__COP_DATASPACE__AUTH__CREDENTIALS__PASSWORD"] = CDSE_PW

def s3_connect():
    S3_HOST = os.environ["S3_HOST"]
    S3_KEY = os.environ["S3_KEY"]
    S3_SECRET = os.environ["S3_PW"]
    s3 = boto3.client(
        "s3",
        endpoint_url=S3_HOST,
        aws_access_key_id=S3_KEY,
        aws_secret_access_key=S3_SECRET,
    )
    return s3

def check_bucket(s3, file, S3_BUCKET="eodag"):
    if "/" in file:
        filepath = file
    elif "MSIL1C" in file:
        filepath = f"cop_dataspace/S2_MSI_L1C/{file}"
    elif "MSIL2A" in file:
        filepath = f"cop_dataspace/S2_MSI_L2A/{file}"
    else:
        filepath = file

    try:
        s3.head_object(Bucket=S3_BUCKET, Key=filepath)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey"):
            return False
        raise

def get_results(collection, product_id, provider="cop_dataspace", dag_run_id="eodag-dag-id"):
    dag = EODataAccessGateway()
    results = dag.search(
        provider=provider,
        collection=collection,
        product_id=product_id
    )
    return results[0]

def stream_results(s3, product, S3_BUCKET="eodag", CHUNK_SIZE=8388608):
    stream = product.stream_download()
    s3_key = "{}/{}".format(
        product.properties["title"],
        stream.filename
    )
    with tqdm(unit="B", unit_scale=True) as pbar:
        s3.upload_fileobj(
            stream.content,
            S3_BUCKET,
            s3_key,
            Config=boto3.s3.transfer.TransferConfig(multipart_threshold=CHUNK_SIZE),
            Callback=pbar.update
        )
    return True

