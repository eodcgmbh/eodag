import os
import time
import boto3
import requests
from botocore.exceptions import ClientError
from eodag import EODataAccessGateway
from tqdm.auto import tqdm

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

def get_eodag_results(provider=None, collection=None, start=None, end=None, geom=None):
    if not provider:
        provider = os.environ["PROVIDER"]
    if not collection:
        collection = os.environ["COLLECTION"]
    if not start:
        start = os.environ["start"]
    if not end:
        end = os.environ["end"]
    if not geom:
        geom = os.environ["geom"]
    dag = EODataAccessGateway()
    results = dag.search(
        provider=provider,
        collection=collection,
        start=start,
        end=end,
        geom=geom
    )
    return results

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


class EarthdataSession(requests.Session):
    AUTH_HOST = "urs.earthdata.nasa.gov"

    def __init__(self, username, password):
        super().__init__()
        self.auth = (username, password)

    def rebuild_auth(self, prepared_request, response):
        if "Authorization" in prepared_request.headers:
            original = requests.utils.urlparse(response.request.url).hostname
            redirect = requests.utils.urlparse(prepared_request.url).hostname
            if original != redirect and redirect != self.AUTH_HOST:
                del prepared_request.headers["Authorization"]

def get_earthdata_result(product_id=None, provider=None, collection=None, filetype="data#", end=".h5"):
    if not product_id:
        product_id = os.environ["PRODUCT_ID"]
    if not provider:
        provider = os.environ["PROVIDER"]
    if not collection:
        collection = os.environ["COLLECTION"]
    if product_id.endswith(".png") or filetype=="browse#":
        filetype="browse#"
        end = ".png"
    if product_id.endswith("_BROWSE.png"):
        filetype="browse#"
        end = "_BROWSE.png"
    product_id = product_id.replace(end, "")
    url = "https://cmr.earthdata.nasa.gov/search"
    cid = requests.get(f"{url}/collections.json?keyword={collection}").json()["feed"]["entry"][0]["id"]
    js = requests.get(f"{url}/granules.json?collection_concept_id={cid}&producer_granule_id[]={product_id}").json()
    js_feed = js.get("feed", {})
    feats = js_feed.get("entry", []) or []
    if len(feats) == 1:
        for l in feats[0]["links"]:
            if l["rel"].endswith(filetype) and l["href"].startswith("https") and l["href"].endswith(end):
                url = l["href"]
                break
    else:
        print("Product not found. Check url: ", f"{url}/granules.json?collection_concept_id={cid}&producer_granule_id[]={product_id}")
        print(feats)
        raise Exception(f"Product not found: {product_id}")
    return url

def stream_earthdata_s3(s3, url, S3_BUCKET="eodag"):
    earthdata_username = os.environ["EARTHDATA_USERNAME"]
    earthdata_password = os.environ["EARTHDATA_PASSWORD"]
    provider = os.environ["PROVIDER"]
    collection = os.environ["COLLECTION"]
    filename = url.split("/")[-1]
    if " " in collection or "/" in collection:
        collection = collection.replace(" ", "_").replace("/", "_")
    s3_target = f"{provider}/{collection}/{filename}"
    if url.endswith(".h5"):
        with EarthdataSession(earthdata_username, earthdata_password) as session:
            with session.get(url, stream=True, timeout=30) as r:
                r.raise_for_status()
                print(f"Status: {r.status_code}")
                mpu = s3.create_multipart_upload(Bucket=S3_BUCKET, Key=s3_target)
                upload_id = mpu["UploadId"]
                parts = []
                part_number = 1
                buffer = b""
                MIN_PART_SIZE = 50 * 1024 * 1024
                try:
                    for chunk in r.iter_content(chunk_size=10 * 1024 * 1024):
                        buffer += chunk
                        if len(buffer) >= MIN_PART_SIZE:
                            part = s3.upload_part(
                                Bucket=S3_BUCKET,
                                Key=s3_target,
                                PartNumber=part_number,
                                UploadId=upload_id,
                                Body=buffer,
                            )
                            parts.append({"PartNumber": part_number, "ETag": part["ETag"]})
                            print(f"Uploaded part {part_number} ({len(buffer) / 1024 / 1024:.1f} MB)")
                            part_number += 1
                            buffer = b""
                    if buffer:
                        part = s3.upload_part(
                            Bucket=S3_BUCKET,
                            Key=s3_target,
                            PartNumber=part_number,
                            UploadId=upload_id,
                            Body=buffer,
                        )
                        parts.append({"PartNumber": part_number, "ETag": part["ETag"]})
                    s3.complete_multipart_upload(
                        Bucket=S3_BUCKET,
                        Key=s3_target,
                        UploadId=upload_id,
                        MultipartUpload={"Parts": parts},
                    )
                    print(f"Uploaded to s3://{S3_BUCKET}/{s3_target}")
                except Exception as e:
                    s3.abort_multipart_upload(Bucket=S3_BUCKET, Key=s3_target, UploadId=upload_id)
                    raise e
    elif url.endswith(".png"):
        r = requests.get(url, auth=(earthdata_username, earthdata_password), stream=True)
        s3.upload_fileobj(
            Fileobj=r.raw,
            Bucket=S3_BUCKET,
            Key=s3_target
        )
        print(f"Uploaded to s3://{S3_BUCKET}/{s3_target}")


def get_cds_result(product_id=None, provider=None, collection=None, end=".nc"):
    import cdsapi
    import re

    if not product_id:
        product_id = os.environ["PRODUCT_ID"]
    if not provider:
        provider = os.environ["PROVIDER"]
    if not collection:
        collection = os.environ["COLLECTION"]

    if provider in ["cop_cds"]:
        url = "https://cds.climate.copernicus.eu/api"
    elif provider in ["cop_ads"]:
        url = "https://ads.atmosphere.copernicus.eu/api"
    os.environ["CDSAPI_URL"] = url

    client = cdsapi.Client()

    re_str = re.search(
        r"^([a-zA-Z0-9\-]+)_(\-?\d+\.\d+)_(\-?\d+\.\d+)_(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})_(.+)$",
        product_id
    )

    if re_str:
        dataset = re_str.group(1)
        x, y = float(re_str.group(2)), float(re_str.group(3))
        date = re_str.group(4) + "/" +  re_str.group(5)
        variable = re_str.group(6)
        if end in variable:
            variable = variable.replace(end, "")
        if end ==".nc":
            data_format = "netcdf"

        request = {
            "variable": [variable],
            "location": {"longitude": x, "latitude": y},
            "date": [date],
            "data_format": data_format
        }
    else:
        from datetime import datetime
        import requests

        re_str = re.search(
            r"^([a-zA-Z0-9\-]+)_(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})_(month_average|10_day_average|daily)?_?(v\d*)_(active|passive|combined)?_?([a-z]*)_(.+)$",
            product_id
        )
        if not re_str:
            return
        dataset = re_str.group(1)
        start, end = re_str.group(2), re_str.group(3)
        year = []
        month = []
        day = []
        time_aggregation = re_str.group(4)
        version = re_str.group(5)
        type_of_sensor = re_str.group(6)
        type_of_record = re_str.group(7)
        variable = re_str.group(8)
        if ".nc" in variable:
            variable = variable.replace(".nc", "")

        url = f"https://cds.climate.copernicus.eu/api/catalogue/v1/collections/{dataset}/constraints.json"
        token = os.environ.get("CDSAPI_KEY")
        resp = requests.get(url, headers={"PRIVATE-TOKEN": token}, timeout=60)
        constraints = resp.json()

        for entry in constraints:
            if variable in entry["variable"] and version in entry["version"] and type_of_record in entry["type_of_record"]:
                if time_aggregation and not time_aggregation in entry["time_aggregation"]:
                    continue
                if type_of_sensor and type_of_sensor in entry["type_of_sensor"]:
                    continue
                years = entry["year"]
                months = entry["month"]
                days = entry["day"]
                break

        start = datetime.fromisoformat(start)
        end = datetime.fromisoformat(end)
        year = [y for y in years if int(start.year) <= int(y) and int(y) <= int(end.year)]

        if len(year) > 2:
            month = months
        elif len(year) == 2:
            month = [m for m in months if int(start.month) <= int(m) or int(m) <= int(end.month)]
        else:
            month = [m for m in months if int(start.month) <= int(m) and int(m) <= int(end.month)]

        if len(year) > 2 or len(month) > 2:
            day = days
        elif len(month) == 2:
            day = [d for d in days if int(start.day) <= int(d) or int(d) <= int(end.day)]
        else:
            day = [d for d in days if int(start.day) <= int(d) and int(d) <= int(end.day)]

        request = {
            "variable": [variable],
            "year": year,
            "month": month,
            "day": day,
            "type_of_record": [type_of_record],
            "version": [version]
        }
        if type_of_sensor:
            request["type_of_sensor"] = [type_of_sensor]
        if time_aggregation:
            request["time_aggregation"] = [time_aggregation]

    print(request)
    req = client.retrieve(dataset, request)
    return req.location

def stream_cds_s3(s3, url, S3_BUCKET="eodag"):
    provider = os.environ["PROVIDER"]
    collection = os.environ["COLLECTION"]
    filename = os.environ["PRODUCT_ID"]
    s3_target = f"{provider}/{collection}/{filename}"

    r = requests.get(url, stream=True)
    s3.upload_fileobj(
        Fileobj=r.raw,
        Bucket=S3_BUCKET,
        Key=s3_target
    )
    print(f"Uploaded to s3://{S3_BUCKET}/{s3_target}")

def access(s3, provider=None, s3_bucket="eodag"):
    if not provider:
        provider = os.environ["PROVIDER"]
    if provider in ["cop_dataspace"]:
        product = get_eodag_result()
        stream_eodag_s3(s3, product, S3_BUCKET=s3_bucket)
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

def access_extent(s3, provider=None, start=None, end=None, geom=None, s3_bucket="eodag"):
    if not provider:
        provider = os.environ["PROVIDER"]
    if provider in ["cop_dataspace"]:
        products = get_eodag_results(start=start, end=end, geom=geom)
        for product in products:
            stream_eodag_s3(s3, product, S3_BUCKET=s3_bucket)
            print(f"Uploaded product: {product}")
    else:
        print(f"Not implemented for provider: {provider}")
