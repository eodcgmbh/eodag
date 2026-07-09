import os
import boto3
from botocore.exceptions import ClientError
from eodag import EODataAccessGateway
from tqdm.auto import tqdm

from .collections.cds_access import get_cds_result, stream_cds_s3
from .collections.cop_dataspace_s3 import get_cop_dataspace_s3_result, stream_cop_dataspace_s3
from .collections.earthdata_access import get_earthdata_result, stream_earthdata_s3
from .collections.maap_access import get_maap_result, stream_maap_s3


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
    if " " in collection or "/" in collection:
        collection = collection.replace(" ", "_").replace("/", "_")
    filepath = f"{provider}/{collection}/{product_id}"
    try:
        s3.head_object(Bucket=S3_BUCKET, Key=filepath)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey"):
            return False
        raise


def get_eodag_result(product_id=None, provider=None, collection=None):
    if not product_id:
        product_id = os.environ["PRODUCT_ID"]
    if ".SAFE" in product_id:
        product_id = product_id.replace(".SAFE", "")
    if ".zip" in product_id:
        product_id = product_id.replace(".zip", "")
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


def stream_eodag_s3(s3, product, provider=None, collection=None, S3_BUCKET="eodag", CHUNK_SIZE=8388608):
    stream = product.stream_download()
    if not provider:
        provider = os.environ["PROVIDER"]
    if not collection:
        collection = os.environ["COLLECTION"]
    s3_target = f"{provider}/{collection}/{stream.filename}"
    print(f"Uploading to {s3_target}")
    with tqdm(unit="B", unit_scale=True) as pbar:
        s3.upload_fileobj(
            stream.content,
            Bucket=S3_BUCKET,
            Key=s3_target,
            Config=boto3.s3.transfer.TransferConfig(multipart_threshold=CHUNK_SIZE),
            Callback=pbar.update
        )
    return True


def access(s3, provider=None, s3_bucket="eodag"):
    if not provider:
        provider = os.environ["PROVIDER"]
    if provider in ["cop_dataspace"]:
        product = get_eodag_result()
        stream_eodag_s3(s3, product, S3_BUCKET=s3_bucket)
    elif provider in ["cop_dataspace_s3"]:
        product = get_cop_dataspace_s3_result()
        stream_cop_dataspace_s3(s3, product, S3_BUCKET=s3_bucket)
    elif provider in ["cop_ads", "cop_cds"]:
        product = get_cds_result()
        if not product:
            print(f"Could not upload product for provider: {provider}")
            raise
        stream_cds_s3(s3, product, S3_BUCKET="eodag")
    elif provider in ["nasa"]:
        url = get_earthdata_result()
        stream_earthdata_s3(s3, url, S3_BUCKET="eodag")
    elif provider in ["maap"]:
        url, headers = get_maap_result()
        stream_maap_s3(s3, url, headers, S3_BUCKET="eodag")
    else:
        print(f"Could not upload product for provider: {provider}")
        raise
    print("Uploaded product!")
