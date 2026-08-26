import os
import re
import requests
import boto3

CATALOGUE_URL = "https://catalogue.dataspace.copernicus.eu/odata/v1"
DOWNLOAD_URL = "https://download.dataspace.copernicus.eu/odata/v1"

def file_path(asset, item_id: str, collection_name: str = "SENTINEL-2"):

    def get_product_uuid(product_name: str, collection_name: str = "SENTINEL-2") -> str:
        url = f"{CATALOGUE_URL}/Products?$filter=Collection/Name eq '{collection_name}' and Name eq '{product_name}'"
        r = requests.get(url)
        r.raise_for_status()
        results = r.json()["value"]
        if not results:
            raise ValueError(f"No product found with name: {product_name}")
        return results[0]["Id"]

    def list_nodes(url: str) -> list:
        r = requests.get(url)
        r.raise_for_status()
        return r.json()["result"]

    def walk_safe(product_uuid: str, product_name: str):
        root_url = f"{DOWNLOAD_URL}/Products({product_uuid})/Nodes({product_name})/Nodes"

        def _walk(url: str, path_prefix: str):
            for node in list_nodes(url):
                name = node["Name"]
                path = f"{path_prefix}/{name}"
                children_url = node["Nodes"]["uri"]
                if node.get("ChildrenNumber", 0) > 0:
                    yield from _walk(children_url, path)
                else:
                    yield path

        yield from _walk(root_url, product_name)

    if ".SAFE" not in item_id:
        item_id += ".SAFE"

    uuid = get_product_uuid(item_id, collection_name)

    for file_path in walk_safe(uuid, item_id):
        if asset in file_path:
            return file_path


def aws():
    access_key = os.environ.get("EODAG__COP_DATASPACE_S3__AUTH__CREDENTIALS__AWS_ACCESS_KEY_ID")
    secret_key = os.environ.get("EODAG__COP_DATASPACE_S3__AUTH__CREDENTIALS__AWS_SECRET_ACCESS_KEY")

    s3 = boto3.client(
        "s3",
        endpoint_url="https://eodata.dataspace.copernicus.eu",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )
    return s3


def _lookup_s3path(item_id):
    """Query CDSE's own OData index for item_id's real S3Path, with the
    "eodata/" bucket-name segment stripped (S3Path comes back as e.g.
    "/eodata/Sentinel-3/SYNERGY/.../....SEN3" -- "eodata" is the bucket name
    itself, not part of the key).
    """
    response = requests.get(
        "https://catalogue.dataspace.copernicus.eu/odata/v1/Products",
        params={"$filter": f"contains(Name,'{item_id}')", "$top": 1},
        timeout=30,
    )
    response.raise_for_status()
    results = response.json().get("value", [])
    if not results:
        raise ValueError(f"CDSE has no product matching item_id={item_id!r}")
    return results[0]["S3Path"].removeprefix("/eodata/")


def get_cop_dataspace_s3_whole_product_result(item_id=None):
    """Fallback whole-product fetch for collections eodag's own search
    doesn't support at all under cop_dataspace (some auxiliary/orbit data
    types have zero product-type registration there) -- bypasses eodag's
    search machinery entirely, using the same OData S3Path lookup as
    get_cop_dataspace_s3_asset_result(), and fetches the product's real S3
    object directly.

    Only handles single-file products (S3Path points directly at a file, not
    a folder); folder-based products (.SAFE/.SEN3 directories of many files)
    need eodag's own search+download support instead, since there's no single
    object to fetch and no generically-correct way to reconstruct/zip the
    folder here.
    """
    if not item_id:
        item_id = os.environ["ITEM_ID"]

    key_or_prefix = _lookup_s3path(item_id)
    if "." not in key_or_prefix.rsplit("/", 1)[-1]:
        raise ValueError(
            f"{item_id!r} is a folder-based product (S3 prefix {key_or_prefix!r}), "
            "not a single file -- the whole-product fallback only supports "
            "single-file products; this collection needs eodag's own "
            "search+download support instead."
        )

    s3_aws = aws()
    return s3_aws.get_object(Bucket="eodata", Key=key_or_prefix)["Body"]


def get_cop_dataspace_s3_asset_result(product_id=None, item_id=None):
    """Mission-agnostic per-asset resolver for cop_dataspace_s3.

    Looks up the product's real S3 folder directly from CDSE's own OData
    index (the S3Path attribute), keyed on item_id, then lists that folder
    and matches the requested asset by filename. No per-mission
    path-building knowledge needed, so this works for any collection whose
    product is a directory of individual files on CDSE's S3 (Sentinel-3's
    flat .SEN3 layout, a nested layout like Sentinel-1's .SAFE too, since S3
    prefix listing matches recursively and only the filename -- not its
    subpath -- is compared).

    PRODUCT_ID here is `{item_id}_{asset_name}` (the router's generic join,
    e.g. "..._Syn_Oa09_reflectance.nc") -- item_id is stripped back off to
    recover the real CDSE-internal filename to search for.
    """
    if not product_id:
        product_id = os.environ["PRODUCT_ID"]
    if not item_id:
        item_id = os.environ["ITEM_ID"]

    asset_name = product_id.removeprefix(f"{item_id}_")
    key_or_prefix = _lookup_s3path(item_id)

    s3_aws = aws()
    if key_or_prefix.split("/")[-1] == asset_name:
        # Single-file products (e.g. a flat .nc file: S3Path points directly
        # at the file, not a folder) -- nothing to list, the path already is
        # the key.
        key = key_or_prefix
    else:
        prefix = key_or_prefix + "/"
        listing = s3_aws.list_objects_v2(Bucket="eodata", Prefix=prefix, MaxKeys=1000)
        for content in listing.get("Contents", []):
            if content["Key"].split("/")[-1] == asset_name:
                key = content["Key"]
                break
        else:
            raise ValueError(f"Could not find asset {asset_name!r} under {prefix}")

    return s3_aws.get_object(Bucket="eodata", Key=key)["Body"]


def get_cop_dataspace_s3_result(product_id=None):
    if not product_id:
        product_id = os.environ["ITEM_ID"] + "_" + os.environ["PRODUCT_ID"]

    s3_aws = aws()

    re_str = re.search(
        r"^(S2A|S2B|S2C|S2D)_(MSIL1C|MSIL2A)_(\d{8}T\d{6})_(N\d{4})_(R\d{3})_(.{6})_(\d{8}T\d{6})",
        product_id
    )

    path = ""
    if not re_str:
        print(f"Could not resolve string for item: {product_id}.")
        raise

    dataset = re_str.group(1)
    if dataset.startswith("S2"):
        path = "Sentinel-2/"
    sub_path = re_str.group(2)
    path = path + sub_path[:3] + "/" + sub_path[3:] + "/"
    datetime_ = re_str.group(3)
    path = path + datetime_[:4] + "/" + datetime_[4:6] + "/" + datetime_[6:8] + "/"
    path = path + re_str.group() + ".SAFE" + "/" 

    try:
        response = s3_aws.list_objects_v2(
            Bucket="eodata",
            Prefix=path,
            MaxKeys=100
        )
    except Exception as e:
        print(f"Could not list objects for {e}")

    for content in response.get("Contents", []):
        key = content["Key"].split("/")[-1]
        if datetime_ in key:
            key = key.split(datetime_+"_")[-1]
        if product_id.endswith(key):
            result = content["Key"]
            print("RESULT: ", result)
            break
        elif product_id.split("_")[-1] in ["B05.jp2", "B06.jp2", "B07.jp2", "B8A.jp2", "B11.jp2", "B12.jp2"]:
            product_id_20 = product_id.replace(".jp2", "_20m.jp2")
            if product_id_20.endswith(key):
                result = content["Key"]
                print("RESULT: ", result)
                break
        elif product_id.split("_")[-1] in ["B01.jp2", "B09.jp2"]:
            product_id_60 = product_id.replace(".jp2", "_60m.jp2")
            if product_id_60.endswith(key):
                result = content["Key"]
                print("RESULT: ", result)
                break
        elif product_id.split("_")[-1] in ["B02.jp2", "B03.jp2", "B04.jp2", "B08.jp2", "TCI.jp2"]:
            product_id_10 = product_id.replace(".jp2", "_10m.jp2")
            if product_id_10.endswith(key):
                result = content["Key"]
                print("RESULT: ", result)
                break
    else:
        print(f"Could not find item: {product_id} where s3 bucket contains: {[content["Key"] for content in response.get("Contents", [])]}")
        return False

    product = s3_aws.get_object(Bucket="eodata", Key=result)["Body"]
    return product


def stream_cop_dataspace_s3(s3_eodc, product, S3_BUCKET, product_id = None, provider=None, collection=None, item_id=None):
    if not product_id:
        product_id = os.environ["PRODUCT_ID"]
    if not item_id:
        item_id = os.environ["ITEM_ID"]
    if not provider:
        provider = os.environ["PROVIDER"]
    if not collection:
        collection = os.environ["COLLECTION"]
    if product_id.endswith(".jp2") and not product_id.startswith("MSK"):
        re_str = re.search(
            r"^(S2A|S2B|S2C|S2D)_(MSIL1C|MSIL2A)_(\d{8}T\d{6})_(N\d{4})_(R\d{3})_(.{6})_(\d{8}T\d{6})",
            item_id
        )
        tile = re_str.group(6)
        date = re_str.group(3)
        if collection in ["S2_MSI_L2A"]:
            if product_id.split("_")[-1] in ["B05.jp2", "B06.jp2", "B07.jp2", "B8A.jp2", "B11.jp2", "B12.jp2"]:
                product_id = product_id.replace(".jp2", "_20m.jp2")
            if product_id.split("_")[-1] in ["B01.jp2", "B09.jp2"]:
                product_id = product_id.replace(".jp2", "_60m.jp2")
            if product_id.split("_")[-1] in ["B02.jp2", "B03.jp2", "B04.jp2", "B08.jp2", "TCI.jp2"]:
                product_id = product_id.replace(".jp2", "_10m.jp2")
        if not tile in product_id and not date in product_id:
            product_id = f"{tile}_{date}_{product_id}"
        product_path = file_path(product_id, item_id)
        s3_target = f"{provider}/{collection}/{item_id.replace('.SAFE', '')}/{product_path}"
    else:
        # Other missions' assets are already resolved to their exact real S3
        # object (via the OData S3Path-driven lookup in
        # get_cop_dataspace_s3_asset_result()/get_cop_dataspace_s3_whole_product_result())
        # before reaching here, so no further per-mission path
        # reconstruction is needed -- upload under a plain, predictable key.
        s3_target = f"{provider}/{collection}/{item_id.replace('.SAFE', '')}/{product_id}"
    s3_eodc.upload_fileobj(product, Bucket=S3_BUCKET, Key=s3_target)
    print(f"Target path: {s3_target}")
    return
