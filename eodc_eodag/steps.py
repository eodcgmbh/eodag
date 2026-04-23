import os
import time
import zipfile
import requests
from utils import s3_connect, set_env

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
    username = os.environ["EODAG__COP_DATASPACE__AUTH__CREDENTIALS__USER"]
    password = os.environ["EODAG__COP_DATASPACE__AUTH__CREDENTIALS__PASSWORD"]
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
