import functools
import os
import boto3
import eodag as _eodag_pkg
import requests
import yaml
from botocore.exceptions import ClientError
from eodag import EODataAccessGateway
from tqdm.auto import tqdm

from .collections.cds_access import get_cds_result, stream_cds_s3
from .collections.cop_dataspace_s3 import (
    get_cop_dataspace_s3_result,
    get_cop_dataspace_s3_asset_result,
    get_cop_dataspace_s3_whole_product_result,
    stream_cop_dataspace_s3,
)
from .collections.earthdata_access import get_earthdata_result, stream_earthdata_s3
from .collections.maap_access import get_maap_result, stream_maap_s3
from .collections.asf_access import get_asf_result, stream_asf_s3


def _normalize_product_id(pid: str) -> str:
    return pid.removesuffix(".zip").removesuffix(".SAFE")


@functools.lru_cache(maxsize=1)
def _cop_dataspace_products():
    path = os.path.join(
        os.path.dirname(_eodag_pkg.__file__), "resources", "providers", "cop_dataspace.yml"
    )
    with open(path) as f:
        return yaml.safe_load(f)["cop_dataspace"]["products"]


@functools.lru_cache(maxsize=1)
def _cop_dataspace_product_type_by_cdse_type():
    # Maps CDSE's OData productType (e.g. "L2__NO2___") to eodag's own
    # product-type ID (e.g. "S5P_L2_NO2") -- needed since several STAC
    # collections are umbrellas over multiple distinct eodag product types.
    return {
        entry["product:type"]: product_type_id
        for product_type_id, entry in _cop_dataspace_products().items()
        if "product:type" in entry
    }


@functools.lru_cache(maxsize=1)
def _cop_dataspace_known_product_types():
    return set(_cop_dataspace_products().keys())


# CDSE's productType for these has no exact match in eodag's own config (e.g.
# S1 COG variants report a mode-specific value eodag's generic placeholder
# never matches). Safe fallback since eodag's download uses id=, not
# productType, so any same-family key works.
_COP_DATASPACE_FALLBACK_PRODUCT_TYPE = {
    "S1_RAW": "S1_SAR_RAW",
    "S1_SAR_COG": "S1_SAR_GRD_COG",
}


def resolve_eodag_collection(collection, product_id):
    fallback = _COP_DATASPACE_FALLBACK_PRODUCT_TYPE.get(collection, collection)
    try:
        response = requests.get(
            "https://catalogue.dataspace.copernicus.eu/odata/v1/Products",
            params={
                "$filter": f"contains(Name,'{product_id}')",
                "$top": 1,
                "$expand": "Attributes",
            },
            timeout=30,
        )
        response.raise_for_status()
        results = response.json().get("value", [])
        if not results:
            return fallback
        for attr in results[0].get("Attributes", []):
            if attr.get("Name") == "productType":
                return _cop_dataspace_product_type_by_cdse_type().get(attr["Value"], fallback)
    except Exception:
        pass
    return fallback


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
    # Returns None (not an error) when eodag has no product-type registration
    # for this collection at all -- caller falls back to a non-eodag path.
    if not product_id:
        product_id = os.environ["PRODUCT_ID"]
    if ".SAFE" in product_id:
        product_id = product_id.replace(".SAFE", "")
    elif ".zip" in product_id:
        product_id = product_id.replace(".zip", "")
    else:
        product_id = os.environ["ITEM_ID"]
    print(f"Product ID: {product_id}")
    if not provider:
        provider = os.environ["PROVIDER"]
    if not collection:
        collection = os.environ["COLLECTION"]
    resolved_collection = resolve_eodag_collection(collection, product_id)
    if provider == "cop_dataspace" and resolved_collection not in _cop_dataspace_known_product_types():
        return None
    dag = EODataAccessGateway()
    results = dag.search(
        provider=provider,
        collection=resolved_collection,
        id=product_id
    )
    return results[0]


def stream_eodag_s3(s3, product, provider=None, collection=None, S3_BUCKET="eodag", CHUNK_SIZE=8388608, item_id=None):
    stream = product.stream_download()
    if not provider:
        provider = os.environ["PROVIDER"]
    if not collection:
        collection = os.environ["COLLECTION"]
    if not item_id:
        item_id = os.environ["ITEM_ID"]
    s3_target = f"{provider}/{collection}/{item_id}/{stream.filename}"
    print(f"Uploading to {s3_target}")
    with tqdm(unit="B", unit_scale=True) as pbar:
        s3.upload_fileobj(
            stream.content,
            Bucket=S3_BUCKET,
            Key=s3_target,
            Config=boto3.s3.transfer.TransferConfig(multipart_threshold=CHUNK_SIZE),
            Callback=pbar.update
        )
    return s3_target


def open_zip(s3, zip_product, provider=None, collection=None, item_id=None,
             s3_bucket="eodag", target_provider="cop_dataspace_s3",
             CHUNK_SIZE=8388608):
    import zipfile
    import tempfile

    if not provider:
        provider = os.environ["PROVIDER"]
    if not collection:
        collection = os.environ["COLLECTION"]
    if not item_id:
        item_id = os.environ["ITEM_ID"]

    with tempfile.TemporaryDirectory() as tmpdir:
        local_zip = os.path.join(tmpdir, f"{item_id}.zip")

        # streams to disk in chunks internally — no full read into memory
        print(f"Downloading {zip_product} to disk")
        s3.download_file(s3_bucket, zip_product, local_zip)

        with zipfile.ZipFile(local_zip, "r") as z:
            for name in z.namelist():
                if name.endswith("/"):
                    continue
                s3_target = f"{target_provider}/{collection}/{item_id}/{name}"
                with z.open(name) as member:
                    s3.upload_fileobj(
                        member,
                        Bucket=s3_bucket,
                        Key=s3_target,
                        Config=boto3.s3.transfer.TransferConfig(multipart_threshold=CHUNK_SIZE),
                    )
                print(f"Unzipped: {s3_target}")


def access(s3, provider=None, s3_bucket="eodag"):
    collection = os.environ.get("COLLECTION", "")
    if not provider:
        provider = os.environ["PROVIDER"]

    # Multi-provider fallback, downloadLink only -- per-asset requests use
    # "cop_dataspace_s3" and fall through to the standard dispatch below.
    if collection == "S1_SAR_GRD" and provider == "cop_dataspace":
        product_id = _normalize_product_id(os.environ["PRODUCT_ID"])
        dag = EODataAccessGateway()
        results = dag.search(collection=collection, id=product_id, raise_errors=False)
        if results:
            product = results[0]
            if product.provider == "nasa":
                url = get_asf_result(product_id=product_id)
                stream_asf_s3(s3, url, S3_BUCKET=s3_bucket, provider=provider)
            else:
                stream_eodag_s3(s3, product, provider=provider, S3_BUCKET=s3_bucket)
            print("Uploaded product!")
            return
        raise Exception("S1_SAR_GRD: all providers failed")

    if provider in ["cop_dataspace"]:
        product = get_eodag_result()
        if product is not None:
            zip_product = stream_eodag_s3(s3, product, S3_BUCKET=s3_bucket)
            if zip_product.endswith(".zip"):
                open_zip(s3=s3, zip_product=zip_product, s3_bucket=s3_bucket, target_provider="cop_dataspace_s3")
        else:
            product, real_filename = get_cop_dataspace_s3_whole_product_result()
            stream_cop_dataspace_s3(s3, product, S3_BUCKET=s3_bucket, real_key=real_filename)
    elif provider in ["cop_dataspace_s3"]:
        # S2 keeps its existing dedicated resolver; every other collection
        # uses the generic OData-driven one.
        if collection in ["S2_MSI_L1C", "S2_MSI_L2A"]:
            product = get_cop_dataspace_s3_result()
            if product:
                stream_cop_dataspace_s3(s3, product, S3_BUCKET=s3_bucket)
            else:
                print("Product not found. Trying cop_dataspace instead...")
                product = get_eodag_result(provider="cop_dataspace")
                zip_product = stream_eodag_s3(s3, product, provider="cop_dataspace", S3_BUCKET=s3_bucket)
                if zip_product.endswith(".zip"):
                    open_zip(s3=s3, zip_product=zip_product, provider="cop_dataspace", s3_bucket=s3_bucket, target_provider="cop_dataspace_s3")
        else:
            product, real_relative_path = get_cop_dataspace_s3_asset_result()
            stream_cop_dataspace_s3(s3, product, S3_BUCKET=s3_bucket, real_key=real_relative_path)
    elif provider in ["cop_ads", "cop_cds", "cop_ewds"]:
        product = get_cds_result()
        if not product:
            print(f"Could not upload product for provider: {provider}")
            raise
        stream_cds_s3(s3, product, S3_BUCKET="eodag")
    elif provider in ["nasa"]:
        url = get_earthdata_result()
        stream_earthdata_s3(s3, url, S3_BUCKET="eodag")
    elif provider in ["asf"]:
        url = get_asf_result()
        stream_asf_s3(s3, url, S3_BUCKET=s3_bucket)
    elif provider in ["maap"]:
        url, headers = get_maap_result()
        stream_maap_s3(s3, url, headers, S3_BUCKET="eodag")
    else:
        print(f"Could not upload product for provider: {provider}")
        raise
    print("Uploaded product!")
