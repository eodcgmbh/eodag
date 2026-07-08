import os
from eodag import EODataAccessGateway

from .utils import stream_eodag_s3

def get_eodag_results(provider=None, collection=None, start=None, end=None, geom=None):
    if not provider:
        provider = os.environ["PROVIDER"]
    if not collection:
        collection = os.environ["COLLECTION"]
    if not start:
        start = os.environ["start"]
    if not end:
        end = os.environ["end"]
    if not geom:
        geom = os.environ["geom"]
    dag = EODataAccessGateway()
    results = dag.search(
        provider=provider,
        collection=collection,
        start=start,
        end=end,
        geom=geom
    )
    return results


def access_extent(s3, provider=None, start=None, end=None, geom=None, s3_bucket="eodag"):
    if not provider:
        provider = os.environ["PROVIDER"]
    if provider in ["cop_dataspace"]:
        products = get_eodag_results(start=start, end=end, geom=geom)
        for product in products:
            stream_eodag_s3(s3, product, S3_BUCKET=s3_bucket)
            print(f"Uploaded product: {product}")
    else:
        print(f"Not implemented for provider: {provider}")