import os
import boto3
from botocore.exceptions import ClientError
from eodag import EODataAccessGateway
from tqdm.auto import tqdm

def s3_connect():
    S3_HOST = os.environ["S3_HOST"]
    S3_KEY = os.environ["S3_KEY"]
    S3_SECRET = os.environ["S3_SECRET"]
    s3 = boto3.client(
        "s3",
        endpoint_url=S3_HOST,
        aws_access_key_id=S3_KEY,
        aws_secret_access_key=S3_SECRET,
    )
    return s3

def check_bucket(s3, product_id=None, provider=None, collection=None, S3_BUCKET="eodag"):
    if not product_id:
        product_id = os.environ["PRODUCT_ID"]
    if not provider:
        provider = os.environ["PROVIDER"]
    if not collection:
        collection = os.environ["COLLECTION"]
    filepath = f"{provider}/{collection}/{product_id}"
    try:
        s3.head_object(Bucket=S3_BUCKET, Key=filepath)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey"):
            return False
        raise

def get_results(product_id=None, provider=None, collection=None):
    if not product_id:
        product_id = os.environ["PRODUCT_ID"]
    if ".SAFE" in product_id:
        product_id = product_id.replace(".SAFE", "")
    if not provider:
        provider = os.environ["PROVIDER"]
    if not collection:
        collection = os.environ["COLLECTION"]
    dag = EODataAccessGateway()
    results = dag.search(
        provider=provider,
        collection=collection,
        id=product_id
    )
    return results[0]

def stream_results(s3, product, provider=None, collection=None, S3_BUCKET="eodag", CHUNK_SIZE=8388608):
    stream = product.stream_download()
    if not provider:
        provider = os.environ["PROVIDER"]
    if not collection:
        collection = os.environ["COLLECTION"]
    s3_key = f"{provider}/{collection}/{stream.filename}"
    with tqdm(unit="B", unit_scale=True) as pbar:
        s3.upload_fileobj(
            stream.content,
            S3_BUCKET,
            s3_key,
            Config=boto3.s3.transfer.TransferConfig(multipart_threshold=CHUNK_SIZE),
            Callback=pbar.update
        )
    return True

def access():
    s3 = s3_connect()
    if check_bucket(s3):
        print("Product already exists!")
    else:
        product = get_results()
        stream_results(s3, product)
        print("Uploaded product!")