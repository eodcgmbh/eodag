import os
import re
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


def get_cop_dataspace_s3_result(product_id=None):
    if not product_id:
        product_id = os.environ["PRODUCT_ID"]

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


    response = s3_aws.list_objects_v2(
        Bucket="eodata",
        Prefix=path,
        MaxKeys=100
    )

    for content in response.get("Contents", []):
        key = content["Key"].split("/")[-1]
        if datetime_ in key:
            key = key.split(datetime_+"_")[-1]
        if product_id.endswith(key):
            result = content["Key"]
            print("RESULT: ", result)
            break
    else:
        print(f"Could not find item: {product_id} where s3 bucket contains: {[content["Key"] for content in response.get("Contents", [])]}")
        raise

    product = s3_aws.get_object(Bucket="eodata", Key=result)["Body"]
    return product


def stream_cop_dataspace_s3(s3_eodc, product, S3_BUCKET, product_id = None, provider=None, collection=None):
    if not product_id:
        product_id = os.environ["PRODUCT_ID"]
    if not provider:
        provider = os.environ["PROVIDER"]
    if not collection:
        collection = os.environ["COLLECTION"]
    s3_target = f"{provider}/{collection}/{product_id}"
    s3_eodc.upload_fileobj(product, S3_BUCKET, s3_target)
    return
