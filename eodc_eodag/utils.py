import os
import boto3
import requests
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


def get_earthdata_result(product_id=None, provider=None, collection=None):
    if not product_id:
        product_id = os.environ["PRODUCT_ID"]
    if "." in product_id:
        product_id = product_id.split(".")[0]
    if not provider:
        provider = os.environ["PROVIDER"]
    if not collection:
        collection = os.environ["COLLECTION"]
    url = "https://cmr.earthdata.nasa.gov/search"
    cid = requests.get(f"{url}/collections.json?keyword={collection}").json()["feed"]["entry"][0]["id"]
    js = requests.get(f"{url}/granules.json?collection_concept_id={cid}&producer_granule_id[]={product_id}").json()
    js_feed = js.get("feed", {})
    feats = js_feed.get("entry", []) or []
    if len(feats) == 1:
        for l in feats[0]["links"]:
            if l["rel"].endswith("browse#") and l["href"].startswith("https"):
                url = l["href"]
                break
    else:
        if "NISAR" in collection:
            url = f"https://nisar.asf.earthdatacloud.nasa.gov/BROWSE/{collection[:-2]}/{product_id}/{product_id}.png"
        if "OPERA" in collection:
            url = f"https://cumulus.asf.earthdatacloud.nasa.gov/BROWSE/OPERA/{collection[:-3]}/{product_id}/{product_id}_BROWSE.png"
    return url

def upload_stream_to_s3(s3, url, S3_BUCKET="eodag"):
    earthdata_user = os.environ["EARTHDATA_USER"]
    earthdata_password = os.environ["EARTHDATA_USER"]
    response = requests.get(url, auth=(earthdata_user, earthdata_password), stream=True)
    response.raise_for_status()
    provider = os.environ["PROVIDER"]
    collection = os.environ["COLLECTION"]
    filename = url.split("/")[-1]
    s3_target = f"{provider}/{collection}/{filename}"
    print(f"Uploading to {s3_target}")
    s3.upload_fileobj(
        Fileobj=response.raw,
        Bucket=S3_BUCKET,
        Key=s3_target
    )


def access():
    s3 = s3_connect()
    product = get_results()
    stream_results(s3, product)
    print("Uploaded product!")