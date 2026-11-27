import os
import requests
import threading
import time

from boto3.s3.transfer import TransferConfig

MB = 1024 * 1024

TRANSFER_CONFIG = TransferConfig(
    multipart_threshold=50 * MB,
    multipart_chunksize=50 * MB,
    max_concurrency=4,
    use_threads=True,
)

CONTENT_TYPES = {
    ".h5": "application/x-hdf5",
    ".png": "image/png",
    ".jpg": "image/jpeg",
}
 
 
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
    elif product_id.endswith(".jpg") or filetype=="browse#":
        filetype="browse#"
        end = ".jpg"
        if product_id.endswith(".1.jpg"):
            end = ".1.jpg"
        if product_id.startswith("BROWSE_"):
            product_id = product_id.replace("BROWSE_", "")
    if product_id.endswith("_BROWSE.jpg"):
        filetype="browse#"
        end = "_BROWSE.jpg"
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

class ProgressLogger:

    def __init__(self, label, every=250 * MB):
        self.label = label
        self.every = every
        self.seen = 0
        self.next_log = every
        self.start = time.monotonic()
        self.lock = threading.Lock()

    def __call__(self, bytes_amount):
        with self.lock:
            self.seen += bytes_amount
            if self.seen >= self.next_log:
                elapsed = time.monotonic() - self.start
                rate = self.seen / MB / elapsed if elapsed else 0.0
                print(f"{self.label}: {self.seen / MB:,.0f} MB uploaded ({rate:.1f} MB/s)", flush=True)
                self.next_log += self.every

    def done(self):
        elapsed = time.monotonic() - self.start
        print(f"{self.label}: finished, {self.seen / MB:,.1f} MB in {elapsed:.1f}s", flush=True)


def stream_earthdata_s3(s3, url, S3_BUCKET="eodag", config=TRANSFER_CONFIG):
    earthdata_username = os.environ["EARTHDATA_USERNAME"]
    earthdata_password = os.environ["EARTHDATA_PASSWORD"]
    provider = os.environ["PROVIDER"]
    collection = os.environ["COLLECTION"]
    item_id = os.environ["ITEM_ID"]
    filename = url.split("/")[-1]
    if " " in collection or "/" in collection:
        collection = collection.replace(" ", "_").replace("/", "_")
    ext = os.path.splitext(filename)[1].lower()
    if ext not in CONTENT_TYPES:
        raise ValueError(f"Unsupported file type: {filename}")
    s3_target = f"{provider}/{collection}/{item_id}/{filename}"

    with EarthdataSession(earthdata_username, earthdata_password) as session:
        with session.get(url, stream=True, timeout=(10, 60)) as r:
            r.raise_for_status()
            r.raw.decode_content = True

            size = r.headers.get("Content-Length")
            size_txt = f"{int(size) / MB:,.1f} MB" if size else "unknown size"
            print(f"Status: {r.status_code}, {size_txt}", flush=True)

            progress = ProgressLogger(filename)
            s3.upload_fileobj(
                Fileobj=r.raw,
                Bucket=S3_BUCKET,
                Key=s3_target,
                ExtraArgs={"ContentType": CONTENT_TYPES[ext]},
                Config=config,
                Callback=progress,
            )
            progress.done()

    print(f"Uploaded to s3://{S3_BUCKET}/{s3_target}", flush=True)
 