import os
import time
import zipfile
import boto3
import requests
from eodag import EODataAccessGateway
from tqdm.auto import tqdm


def set_env(S3_HOST, S3_KEY, S3_PW, CDSE_USER, CDSE_PW):
    os.environ["S3_HOST"] = S3_HOST
    os.environ["S3_KEY"] = S3_KEY
    os.environ["S3_PW"] = S3_PW
    os.environ["CDSE_USER"] = CDSE_USER
    os.environ["CDSE_PW"] = CDSE_PW

def s3_connect():
    S3_HOST = os.environ["S3_HOST"]
    S3_KEY = os.environ["S3_KEY"]
    S3_SECRET = os.environ["S3_PW"]
    s3 = boto3.client(
        "s3",
        endpoint_url=S3_HOST,
        aws_access_key_id=S3_KEY,
        aws_secret_access_key=S3_SECRET,
    )
    return s3

def check_bucket(s3, file, S3_BUCKET="eodag"):
    if "/" in file:
        filepath = file
    elif "MSIL1C" in file:
        filepath = f"cop_dataspace/S2_MSI_L1C/{file}"
    elif "MSIL2A" in file:
        filepath = f"cop_dataspace/S2_MSI_L2A/{file}"
    else:
        filepath = file

    return s3.head_object(Bucket=S3_BUCKET, Key=filepath)

def get_results(collection, product_id, provider="cop_dataspace", dag_run_id="eodag-dag-id"):
    dag = EODataAccessGateway()
    results = dag.search(
        provider=provider,
        collection=collection,
        product_id=product_id
    )
    return results

def stream_results(s3, results=[], S3_BUCKET="eodag", CHUNK_SIZE=8388608):
    for product in results:
        stream = product.stream_download()
        s3_key = "{}/{}".format(
            product.properties["title"],
            stream.filename
        )
        with tqdm(unit="B", unit_scale=True) as pbar:
            s3.upload_fileobj(
                stream.content,
                S3_BUCKET,
                s3_key,
                Config=boto3.s3.transfer.TransferConfig(multipart_threshold=CHUNK_SIZE),
                Callback=pbar.update
            )
        return s3.list_objects_v2(Bucket=S3_BUCKET).get("Contents", [])


def upload_file(file, s3, S3_BUCKET="eodag"):
    filename = file.split("/")[-1]
    if "MSIL1C" in filename:
        filepath = f"cop_dataspace/S2_MSI_L1C/{filename}"
    elif "MSIL2A" in filename:
        filepath = f"cop_dataspace/S2_MSI_L2A/{filename}"
    else:
        filepath = file
    s3.upload_file(file, S3_BUCKET, filepath)

def get_token(refresh_token=None):
    username = os.environ["CDSE_USER"]
    password = os.environ["CDSE_PW"]
    token_url = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
    try:
        data = {
            "grant_type": "refresh_token" if refresh_token else "password",
            "client_id": "cdse-public",
            "refresh_token": refresh_token,
            "username": username,
            "password": password,
        }
        res = requests.post(token_url, data=data)
        res.raise_for_status()
        tokens = res.json()
        return tokens["access_token"], tokens.get("refresh_token")
    except Exception as e:
        print(f"Token fetch failed: {e}")
        raise

def refresh(refresh_token):
    token, refresh_token = get_token(refresh_token)
    return refresh_token

def download_file(uuid, outputfile):
    base_url = "https://zipper.dataspace.copernicus.eu/odata/v1/Products"
    url = f"{base_url}({uuid})/$value"
    time.sleep(2)
    token, refresh_token = get_token()
    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {token}"})
    response = session.get(url, stream=True)
    if response.status_code == 401:
        print(
            f"Token expired during download of {outputfile.as_posix()}, refreshing..."
        )
        token = refresh(refresh_token)
        session.headers.update({"Authorization": f"Bearer {token}"})
        response = session.get(url, stream=True)
    if response.status_code != 200:
        print((response.content, outputfile.as_posix()))
    content_length = int(response.headers["Content-Length"])
    if outputfile.exists() and os.stat(outputfile).st_size == content_length:
        return {"path": outputfile.as_posix(), "uuid": uuid}
    if response.status_code == 200:
        with open(outputfile.as_posix(), "wb") as file:
            print(f"Downloading {outputfile.as_posix()}")
            for chunk in response.iter_content(chunk_size=1024 * 1024 * 5):
                if chunk:
                    file.write(chunk)
        if os.stat(outputfile).st_size != content_length:
            raise ValueError(
                (
                    "File size of downloaded file does not match content-length",
                    outputfile.as_posix(),
                )
            )
        if not zipfile.is_zipfile(outputfile):
            raise ValueError(("File is not a valid zip file!", outputfile.as_posix()))
        return {"path": outputfile.as_posix(), "uuid": uuid}
