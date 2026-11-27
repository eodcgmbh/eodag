import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse

import requests

MB = 1024 * 1024
MAX_RETRIES = 5
TIMEOUT = (10, 60)  # connect, read
CONTENT_TYPES = {".h5": "application/x-hdf5", ".png": "image/png", ".jpg": "image/jpeg"}


class EarthdataSession(requests.Session):
    AUTH_HOST = "urs.earthdata.nasa.gov"

    def __init__(self, username, password):
        super().__init__()
        self.auth = (username, password)

    def rebuild_auth(self, prepared_request, response):
        if "Authorization" in prepared_request.headers:
            original = urlparse(response.request.url).hostname
            redirect = urlparse(prepared_request.url).hostname
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

def stream_earthdata_s3(s3, url, S3_BUCKET="eodag"):
    earthdata_username = os.environ["EARTHDATA_USERNAME"]
    earthdata_password = os.environ["EARTHDATA_PASSWORD"]
    provider = os.environ["PROVIDER"]
    collection = os.environ["COLLECTION"]
    item_id = os.environ["ITEM_ID"]
    filename = url.split("/")[-1]
    if " " in collection or "/" in collection:
        collection = collection.replace(" ", "_").replace("/", "_")
    s3_target = f"{provider}/{collection}/{item_id}/{filename}"
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
                    t_dl = t_up = 0.0
                    mark = time.monotonic()
                    for chunk in r.iter_content(chunk_size=10 * 1024 * 1024):
                        t_dl += time.monotonic() - mark
                        buffer += chunk
                        if len(buffer) >= MIN_PART_SIZE:
                            t0 = time.monotonic()
                            part = s3.upload_part(Bucket=S3_BUCKET, Key=s3_target, PartNumber=part_number,
                                                UploadId=upload_id, Body=buffer)
                            t_up += time.monotonic() - t0
                            parts.append({"PartNumber": part_number, "ETag": part["ETag"]})
                            print(f"part {part_number}: download {t_dl:.0f}s, upload {t_up:.0f}s (cumulative)", flush=True)
                            part_number += 1
                            buffer = b""
                        mark = time.monotonic()
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
    elif url.endswith(".jpg"):
        r = requests.get(url, auth=(earthdata_username, earthdata_password), stream=True)
        s3.upload_fileobj(
            Fileobj=r.raw,
            Bucket=S3_BUCKET,
            Key=s3_target
        )
        print(f"Uploaded to s3://{S3_BUCKET}/{s3_target}")

class RangesNotSupported(Exception):
    """The server does not answer byte-range requests; fall back to the sequential upload."""


class RangeSource:
    """
    Fetches byte ranges of one Earthdata file from several threads.

    The Earthdata login and redirects are done once; afterwards the threads request the final
    (usually pre-signed) download URL directly. If that URL expires (401/403), the request goes
    through the login again and the new final URL is shared with all threads.
    """

    def __init__(self, url, username, password):
        self.original = url
        self.username = username
        self.password = password
        self.lock = threading.Lock()
        self.local = threading.local()

        r = self._via_login(0, 0)
        try:
            r.raise_for_status()
            content_range = r.headers.get("Content-Range", "")
            total = content_range.rsplit("/", 1)[-1]
            if r.status_code != 206 or not total.isdigit():
                raise RangesNotSupported(f"No byte-range support for {url} (status {r.status_code})")
            self.size = int(total)
        finally:
            r.close()

    def _session(self, kind):
        session = getattr(self.local, kind, None)
        if session is None:
            if kind == "login":
                session = EarthdataSession(self.username, self.password)
            else:
                session = requests.Session()
            setattr(self.local, kind, session)
        return session

    def _via_login(self, start, end):
        r = self._session("login").get(
            self.original, headers={"Range": f"bytes={start}-{end}"}, timeout=TIMEOUT
        )
        if r.ok:
            with self.lock:
                self.final = r.url
                # No redirect away from the original host: every request has to carry the login.
                self.use_login = urlparse(r.url).hostname == urlparse(self.original).hostname
        return r

    def get_range(self, start, end):
        if not self.use_login:
            r = self._session("plain").get(
                self.final, headers={"Range": f"bytes={start}-{end}"}, timeout=TIMEOUT
            )
            if r.status_code not in (401, 403):
                return r
            r.close()
        return self._via_login(start, end)


def _transfer_part(src, s3, bucket, key, upload_id, number, start, end):
    expected = end - start + 1
    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = src.get_range(start, end)
            r.raise_for_status()
            body = r.content
            if r.status_code != 206 or len(body) != expected:
                raise IOError(f"got {len(body)} of {expected} bytes (status {r.status_code})")
            break
        except (requests.RequestException, IOError) as e:
            last_error = e
            print(f"part {number}: attempt {attempt} failed: {e}", flush=True)
            time.sleep(min(2 ** attempt, 30))
    else:
        raise last_error

    part = s3.upload_part(Bucket=bucket, Key=key, PartNumber=number, UploadId=upload_id, Body=body)
    return {"PartNumber": number, "ETag": part["ETag"]}, expected


def stream_earthdata_s3_parallel(s3, url, S3_BUCKET="eodag", part_size=50 * MB, workers=8):
    username = os.environ["EARTHDATA_USERNAME"]
    password = os.environ["EARTHDATA_PASSWORD"]
    provider = os.environ["PROVIDER"]
    collection = os.environ["COLLECTION"].replace(" ", "_").replace("/", "_")
    item_id = os.environ["ITEM_ID"]

    filename = url.split("/")[-1]
    ext = os.path.splitext(filename)[1].lower()
    if ext not in CONTENT_TYPES:
        raise ValueError(f"Unsupported file type: {filename}")
    s3_target = f"{provider}/{collection}/{item_id}/{filename}"

    src = RangeSource(url, username, password)
    size = src.size

    # Small files (browse images): one request, one put.
    if size <= part_size:
        r = src.get_range(0, size - 1)
        r.raise_for_status()
        s3.put_object(Bucket=S3_BUCKET, Key=s3_target, Body=r.content, ContentType=CONTENT_TYPES[ext])
        print(f"Uploaded to s3://{S3_BUCKET}/{s3_target}", flush=True)
        return

    ranges = [
        (number, start, min(start + part_size, size) - 1)
        for number, start in enumerate(range(0, size, part_size), start=1)
    ]
    if len(ranges) > 10_000:
        raise ValueError(f"{len(ranges)} parts exceed the S3 limit of 10,000; increase part_size")
    print(f"{filename}: {size / MB:,.1f} MB in {len(ranges)} parts, {workers} workers", flush=True)

    mpu = s3.create_multipart_upload(Bucket=S3_BUCKET, Key=s3_target, ContentType=CONTENT_TYPES[ext])
    upload_id = mpu["UploadId"]
    parts, done, t0 = [], 0, time.monotonic()
    pool = ThreadPoolExecutor(max_workers=workers)
    try:
        futures = [
            pool.submit(_transfer_part, src, s3, S3_BUCKET, s3_target, upload_id, n, start, end)
            for n, start, end in ranges
        ]
        for future in as_completed(futures):
            part, nbytes = future.result()
            parts.append(part)
            done += nbytes
            if len(parts) % 10 == 0 or len(parts) == len(ranges):
                elapsed = time.monotonic() - t0
                print(
                    f"{len(parts)}/{len(ranges)} parts, {done / MB:,.0f} MB, "
                    f"{done / MB / elapsed:.1f} MB/s",
                    flush=True,
                )
        parts.sort(key=lambda p: p["PartNumber"])
        s3.complete_multipart_upload(
            Bucket=S3_BUCKET, Key=s3_target, UploadId=upload_id, MultipartUpload={"Parts": parts}
        )
    except BaseException:
        pool.shutdown(wait=True, cancel_futures=True)
        s3.abort_multipart_upload(Bucket=S3_BUCKET, Key=s3_target, UploadId=upload_id)
        raise
    finally:
        pool.shutdown(wait=True)

    elapsed = time.monotonic() - t0
    print(f"Uploaded to s3://{S3_BUCKET}/{s3_target} in {elapsed:.0f}s", flush=True)
