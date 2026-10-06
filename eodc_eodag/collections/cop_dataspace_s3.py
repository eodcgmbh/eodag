import os
import re
import requests
import boto3


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
    # S3Path comes back as "/eodata/Sentinel-3/.../....SEN3" -- "eodata" is the
    # bucket name, not part of the key.
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
    # Fallback for collections eodag has no product-type registration for at
    # all (some AUX/orbit types) -- fetches the real S3 object directly via
    # OData, bypassing eodag's search entirely. Single-file products only;
    # folder-based (.SAFE/.SEN3) products still need eodag's own download.
    if not item_id:
        item_id = os.environ["ITEM_ID"]

    key_or_prefix = _lookup_s3path(item_id)
    filename = key_or_prefix.rsplit("/", 1)[-1]
    if "." not in filename:
        raise ValueError(f"{item_id!r} is folder-based, not a single file")

    s3_aws = aws()
    stream = s3_aws.get_object(Bucket="eodata", Key=key_or_prefix)["Body"]
    return stream, filename


def get_cop_dataspace_s3_asset_result(product_id=None, item_id=None):
    # Mission-agnostic per-asset resolver: looks up the product's real S3
    # folder via OData (no per-mission path-building needed) and matches the
    # requested asset by filename. Returns None (not an error) when the
    # asset genuinely doesn't exist -- a bogus/typo'd name is a normal
    # "not found" outcome, not a system failure.
    if not product_id:
        product_id = os.environ["PRODUCT_ID"]
    if not item_id:
        item_id = os.environ["ITEM_ID"]

    asset_name = product_id.removeprefix(f"{item_id}_")
    key_or_prefix = _lookup_s3path(item_id)

    s3_aws = aws()
    basename = key_or_prefix.split("/")[-1]
    # A non-SAFE/SEN3 flat file IS the whole product when product_id == item_id.
    is_whole_product_request = product_id == item_id
    is_flat_file = "." in basename and not basename.lower().endswith((".safe", ".sen3"))
    if (
        (is_whole_product_request and is_flat_file)
        or basename == asset_name
        or basename.rsplit(".", 1)[-1] == asset_name
    ):
        key = key_or_prefix  # single-file product: S3Path already is the key
        relative_path = basename
    else:
        prefix = key_or_prefix + "/"
        listing = s3_aws.list_objects_v2(Bucket="eodata", Prefix=prefix, MaxKeys=1000)
        for content in listing.get("Contents", []):
            basename = content["Key"].split("/")[-1]
            # eodag-server's own STAC catalog strips the real mission-prefix
            # off some real filenames (e.g. real "s1-product-preview.xsd" is
            # reported as just "product-preview.xsd") -- accept either form.
            if basename == asset_name or basename.endswith(f"-{asset_name}"):
                key = content["Key"]
                relative_path = key[len(prefix):]  # real internal path, e.g. "preview/quick-look.png"
                break
        else:
            return None

    stream = s3_aws.get_object(Bucket="eodata", Key=key)["Body"]
    return stream, relative_path


def get_cop_dataspace_s3_result(item_id=None):
    # Whole-product fetch: lists every real file under the matched SAFE path.
    if not item_id:
        item_id = os.environ["ITEM_ID"]

    re_str = re.search(
        r"^(S2A|S2B|S2C|S2D)_(MSIL1C|MSIL2A)_(\d{8}T\d{6})_(N\d{4})_(R\d{3})_(.{6})_(\d{8}T\d{6})",
        item_id
    )
    if not re_str:
        print(f"Could not resolve string for item: {item_id}.")
        return None

    dataset = re_str.group(1)
    path = "Sentinel-2/" if dataset.startswith("S2") else ""
    sub_path = re_str.group(2)
    path = path + sub_path[:3] + "/" + sub_path[3:] + "/"
    datetime_ = re_str.group(3)
    mission_root = path + datetime_[:4] + "/" + datetime_[4:6] + "/" + datetime_[6:8] + "/"
    path = mission_root + re_str.group() + ".SAFE" + "/"

    s3_aws = aws()
    try:
        paginator = s3_aws.get_paginator("list_objects_v2")
        keys = [
            content["Key"]
            for page in paginator.paginate(Bucket="eodata", Prefix=path)
            for content in page.get("Contents", [])
        ]
    except Exception as e:
        print(f"Could not list objects for {e}")
        return None

    if not keys:
        print(f"Could not find any files for item: {item_id} under {path}")
        return None

    return [(key, key[len(mission_root):]) for key in keys]


def stream_cop_dataspace_s3(s3_eodc, product, S3_BUCKET, product_id = None, provider=None, collection=None, item_id=None, real_key=None):
    if not product_id:
        product_id = os.environ["PRODUCT_ID"]
    if not item_id:
        item_id = os.environ["ITEM_ID"]
    if not provider:
        provider = os.environ["PROVIDER"]
    if not collection:
        collection = os.environ["COLLECTION"]
    if real_key is not None:
        # Rolling Archive's own convention: real filename/relative-path, not
        # this resolver's synthetic flat name.
        s3_target = f"{provider}/{collection}/{item_id.replace('.SAFE', '')}/{real_key}"
    else:
        # Non-S2 assets are already resolved to their real S3 object before
        # reaching here -- no per-mission path reconstruction needed.
        s3_target = f"{provider}/{collection}/{item_id.replace('.SAFE', '')}/{product_id}"
    s3_eodc.upload_fileobj(product, Bucket=S3_BUCKET, Key=s3_target)
    print(f"Target path: {s3_target}")
    return
