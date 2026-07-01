import os
import requests

def get_maap_result(product_id=None, collection=None):
    if not product_id:
        product_id = os.environ["PRODUCT_ID"]
    if not collection:
        collection = os.environ["COLLECTION"]

    maap_token = os.environ.get("MAAP_TOKEN", "")
    headers = {"Authorization": f"Bearer {maap_token}"} if maap_token else {}

    catalog_url = "https://catalog.maap.eo.esa.int"
    r = requests.get(
        f"{catalog_url}/collections/{collection}/items/{product_id}",
        headers=headers,
        timeout=60,
    )
    r.raise_for_status()
    item = r.json()

    asset_href = None
    for _, a in item.get("assets", {}).items():
        mt = (a.get("type") or "").lower()
        href = a.get("href", "")
        if "tiff" in mt or href.lower().endswith((".tif", ".tiff")):
            asset_href = href
            break

    if asset_href is None:
        raise RuntimeError(f"No TIFF/COG assets on {product_id}")

    print(f"Asset: {asset_href}")
    return asset_href