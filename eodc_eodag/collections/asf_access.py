import base64
import os
import boto3
import requests


class ASFSession(requests.Session):
    """Session that re-adds basic auth whenever a redirect goes to urs.earthdata.nasa.gov.

    EarthdataSession strips auth when redirecting away from URS, which breaks ASF's
    multi-hop OAuth flow: sentinel1 → URS (auth sent) → sentinel1/login (auth stripped)
    → sentinel1/file (no auth) → URS again (no auth → 401).
    This class always re-adds credentials when the redirect target is URS.
    """
    AUTH_HOST = "urs.earthdata.nasa.gov"

    def __init__(self, username, password):
        super().__init__()
        self.auth = (username, password)
        self._basic = "Basic " + base64.b64encode(f"{username}:{password}".encode()).decode()

    def rebuild_auth(self, prepared_request, response):
        redirect_host = requests.utils.urlparse(prepared_request.url).hostname
        original_host = requests.utils.urlparse(response.request.url).hostname
        if original_host == redirect_host:
            return
        if redirect_host == self.AUTH_HOST:
            prepared_request.headers["Authorization"] = self._basic
        else:
            prepared_request.headers.pop("Authorization", None)


def get_asf_result(product_id=None):
    if not product_id:
        product_id = os.environ["PRODUCT_ID"]
    product_id = product_id.replace(".zip", "").replace(".SAFE", "")

    js = requests.get(
        "https://cmr.earthdata.nasa.gov/search/granules.json",
        params={"producer_granule_id[]": product_id, "provider": "ASF"},
    ).json()
    feats = js.get("feed", {}).get("entry", [])
    if not feats:
        raise Exception(f"ASF product not found: {product_id}")

    for link in feats[0]["links"]:
        if link["rel"].endswith("data#") and link["href"].endswith(".zip"):
            return link["href"]

    raise Exception(f"No downloadable .zip link found for: {product_id}")


def stream_asf_s3(s3, url, S3_BUCKET="eodag", CHUNK_SIZE=8388608, provider=None):
    username = os.environ["EARTHDATA_USERNAME"]
    password = os.environ["EARTHDATA_PASSWORD"]
    provider = provider or os.environ["PROVIDER"]
    item_id = os.environ["ITEM_ID"]
    collection = os.environ["COLLECTION"]
    if " " in collection or "/" in collection:
        collection = collection.replace(" ", "_").replace("/", "_")
    filename = url.split("/")[-1]
    s3_target = f"{provider}/{collection}/{item_id}/{filename}"
    print(f"Uploading to {s3_target}")
    with ASFSession(username, password) as session:
        with session.get(url, stream=True, timeout=60) as r:
            r.raise_for_status()
            s3.upload_fileobj(
                r.raw,
                Bucket=S3_BUCKET,
                Key=s3_target,
                Config=boto3.s3.transfer.TransferConfig(multipart_threshold=CHUNK_SIZE),
            )
    print(f"Uploaded to s3://{S3_BUCKET}/{s3_target}")
