import os
import requests


def get_keycloak_token(keycloak_url, realm, client_id, client_secret, username, password):
    data = {
        "grant_type": "password",
        "client_id": client_id,
        "client_secret": client_secret,
        "username": username,
        "password": password,
        "scope": "openid",
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}

    response = requests.post(
        f"{keycloak_url}/realms/{realm}/protocol/openid-connect/token",
        data=data,
        headers=headers,
        timeout=30,
    )
    response.raise_for_status()
    return response.json()["access_token"]


def get_maap_result(product_id=None, collection=None):
    if not product_id:
        product_id = os.environ["PRODUCT_ID"]
    if not collection:
        collection = os.environ["COLLECTION"]

    if "/" not in product_id:
        raise ValueError("PRODUCT_ID must be in the format '{item_id}/{asset_key}'")
    item_id, asset_key = product_id.split("/", 1)

    token = get_keycloak_token(
        keycloak_url=os.environ.get("MAAP_IAM_URL", "https://iam.maap.eo.esa.int/"),
        realm=os.environ.get("MAAP_REALM", "esa-maap"),
        client_id=os.environ["MAAP_CLIENT_ID"],
        client_secret=os.environ["MAAP_CLIENT_SECRET"],
        username=os.environ["MAAP_USERNAME"],
        password=os.environ["MAAP_PASSWORD"],
    )
    headers = {"Authorization": f"Bearer {token}"}

    r = requests.get(
        f"https://catalog.maap.eo.esa.int/catalogue/collections/{collection}/items/{item_id}",
        headers=headers,
        timeout=60,
    )
    r.raise_for_status()
    item = r.json()

    print(f"asset_key: {asset_key}")

    assets = item.get("assets", {})
    if asset_key in ("product", "downloadLink"):
        asset = next(
            (
                a for a in assets.values()
                if "archive" in a.get("roles", []) or a.get("type") == "application/zip"
            ),
            None,
        )
    else:
        asset = next(
            (
                a for k, a in assets.items()
                if k == asset_key
                or a.get("file:local_path") == asset_key
                or a.get("title") == asset_key
                or a.get("href", "").endswith(f"/{asset_key}")
            ),
            None,
        )
    if asset is None:
        raise RuntimeError(f"No asset matching '{asset_key}' found in item '{item_id}'")

    href = asset["href"]
    print(f"Asset: {href}")
    return href, headers


def stream_maap_s3(s3, url, headers, S3_BUCKET="eodag"):
    provider = os.environ["PROVIDER"]
    collection = os.environ["COLLECTION"]
    filename = url.split("/")[-1]
    if " " in collection or "/" in collection:
        collection = collection.replace(" ", "_").replace("/", "_")
    s3_target = f"{provider}/{collection}/{filename}"

    r = requests.get(url, headers=headers, stream=True, timeout=180)
    r.raise_for_status()
    s3.upload_fileobj(Fileobj=r.raw, Bucket=S3_BUCKET, Key=s3_target)
    print(f"Uploaded to s3://{S3_BUCKET}/{s3_target}")


if __name__ == "__main__":
    token = get_keycloak_token(
        keycloak_url=os.environ.get("MAAP_IAM_URL", "https://iam.maap.eo.esa.int/"),
        realm=os.environ.get("MAAP_REALM", "esa-maap"),
        client_id=os.environ["MAAP_CLIENT_ID"],
        client_secret=os.environ["MAAP_CLIENT_SECRET"],
        username=os.environ["MAAP_USERNAME"],
        password=os.environ["MAAP_PASSWORD"],
    )
    print("Token:", token)

    url, headers = get_maap_result()
    print("Asset URL:", url)
