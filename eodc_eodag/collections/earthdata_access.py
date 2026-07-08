import os
import requests

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